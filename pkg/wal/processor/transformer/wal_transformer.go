// SPDX-License-Identifier: Apache-2.0

package transformer

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"
	pglib "github.com/xataio/pgstream/internal/postgres"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/transformers"
	"github.com/xataio/pgstream/pkg/transformers/builder"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor"
)

// Transformer is a decorator around a wal processor that transforms wal event
// column values following the configured transformation rules.
type Transformer struct {
	logger         loglib.Logger
	processor      processor.Processor
	transformerMap map[string]columnTransformers
}

type columnTransformers map[string]transformers.Transformer

type Config struct {
	TransformerRulesFile string
}

type Option func(t *Transformer)

// New will return a transformer processor wrapper that will transform incoming
// wal event column values as configured by the transformation rules.
func New(cfg *Config, processor processor.Processor, opts ...Option) (*Transformer, error) {
	rules, err := readRulesFromFile(cfg.TransformerRulesFile)
	if err != nil {
		return nil, err
	}

	transformerMap, err := transformerMapFromRules(rules)
	if err != nil {
		return nil, err
	}

	t := &Transformer{
		logger:         loglib.NewNoopLogger(),
		transformerMap: transformerMap,
		processor:      processor,
	}

	for _, opt := range opts {
		opt(t)
	}

	return t, nil
}

func WithLogger(l loglib.Logger) Option {
	return func(in *Transformer) {
		in.logger = loglib.NewLogger(l).WithFields(loglib.Fields{
			loglib.ModuleField: "wal_transformer",
		})
	}
}

func (t *Transformer) ProcessWALEvent(ctx context.Context, event *wal.Event) error {
	err := t.applyTransformations(event)
	if err != nil {
		return err
	}

	return t.processor.ProcessWALEvent(ctx, event)
}

func (t *Transformer) Name() string {
	return t.processor.Name()
}

func (t *Transformer) Close() error {
	return nil
}

func (t *Transformer) Validate(ctx context.Context, url string) error {
	conn, err := pglib.NewConnPool(ctx, url)
	if err != nil {
		return fmt.Errorf("creating postgres connection pool: %w", err)
	}
	defer conn.Close(context.Background())

	for schemaTable, columnTransformers := range t.transformerMap {
		query := fmt.Sprintf("SELECT * FROM %s LIMIT 0", schemaTable)
		rows, err := conn.Query(ctx, query)
		if err != nil {
			return fmt.Errorf("querying table rows: %w", err)
		}
		defer rows.Close()
		fieldDescriptions := rows.FieldDescriptions()

		// TODO: maybe error out if len(fieldDescriptions) != len(columnTransformers)
		// if we start requiring a transformer for every column (noop transformers)

		// map column names to column pg type OIDs, skip columns that don't have a transformer
		mappedColumns := make(map[string]uint32, len(fieldDescriptions))
		for _, desc := range fieldDescriptions {
			if _, found := columnTransformers[string(desc.Name)]; !found {
				continue
			}

			mappedColumns[string(desc.Name)] = desc.DataTypeOID
		}

		// check that all column transformers are compatible with corresponding column types
		for colName, tr := range columnTransformers {
			datatype, found := mappedColumns[colName]
			if !found {
				return fmt.Errorf("column %s not found in table %s", colName, schemaTable)
			}
			if !columnTransformerCompatibleWithPGType(tr.Type(), datatype) {
				return fmt.Errorf("transformer %s is not compatible with type of column %s", tr.Type(), colName)
			}
		}
	}

	return nil
}

func (t *Transformer) applyTransformations(event *wal.Event) error {
	if event.Data == nil || len(t.transformerMap) == 0 {
		return nil
	}

	columnTransformers, found := t.transformerMap[schemaTableKey(event.Data.Schema, event.Data.Table)]
	if !found || len(columnTransformers) == 0 {
		return nil
	}

	columns := event.Data.Columns
	for i, col := range columns {
		// do not transform nil column values for now
		if col.Value == nil {
			continue
		}

		columnTransformer, found := columnTransformers[col.Name]
		if !found {
			continue
		}

		newValue, err := columnTransformer.Transform(t.getTransformValue(&col, event.Data.Columns))
		if err != nil {
			t.logger.Error(err, "transforming column", loglib.Fields{
				"severity":    "DATALOSS",
				"column_name": col.Name,
				"schema":      event.Data.Schema,
				"table":       event.Data.Table,
			})
			newValue = nil
		}
		t.logger.Trace("applying column transformation", loglib.Fields{"column_name": col.Name, "column_value": col.Value, "column_type": col.Type, "new_column_value": newValue})
		columns[i].Value = newValue
	}

	return nil
}

func (t *Transformer) getTransformValue(column *wal.Column, columns []wal.Column) transformers.Value {
	values := make(map[string]any, len(columns)-1)
	for _, col := range columns {
		if col.Name == column.Name {
			continue
		}
		values[col.Name] = col.Value
	}
	return transformers.NewValue(column.Value, values)
}

func schemaTableKey(schema, table string) string {
	return pglib.QuoteQualifiedIdentifier(schema, table)
}

func transformerMapFromRules(rules *Rules) (map[string]columnTransformers, error) {
	var err error
	transformerMap := map[string]columnTransformers{}
	for _, table := range rules.Transformers {
		schemaTableTransformers := make(map[string]transformers.Transformer)
		transformerMap[schemaTableKey(table.Schema, table.Table)] = schemaTableTransformers
		for colName, transformerRules := range table.ColumnRules {
			schemaTableTransformers[colName], err = builder.New(transformerRulesToConfig(transformerRules))
			if err != nil {
				return nil, err
			}
		}
	}
	return transformerMap, nil
}

func transformerRulesToConfig(rules TransformerRules) *transformers.Config {
	return &transformers.Config{
		Name:              transformers.TransformerType(rules.Name),
		Parameters:        rules.Parameters,
		DynamicParameters: rules.DynamicParameters,
	}
}

func columnTransformerCompatibleWithPGType(t transformers.TransformerType, pgType uint32) bool {
	switch t {
	case transformers.Masking, transformers.PhoneNumber, transformers.String, transformers.NeosyncString, transformers.NeosyncFirstName, transformers.NeosyncEmail, transformers.GreenmaskString, transformers.GreenmaskFirstName, transformers.GreenmaskChoice:
		return pgType == pgtype.TextOID || pgType == pgtype.VarcharOID || pgType == pgtype.BPCharOID
	case transformers.GreenmaskInteger, transformers.GreenmaskUnixTimestamp:
		return pgType == pgtype.Int2OID || pgType == pgtype.Int4OID || pgType == pgtype.Int8OID
	case transformers.GreenmaskFloat:
		return pgType == pgtype.Float4OID || pgType == pgtype.Float8OID
	case transformers.GreenmaskUUID:
		return pgType == pgtype.UUIDOID || pgType == pgtype.TextOID || pgType == pgtype.VarcharOID || pgType == pgtype.BPCharOID
	case transformers.GreenmaskBoolean:
		return pgType == pgtype.BoolOID
	case transformers.GreenmaskDate:
		return pgType == pgtype.DateOID || pgType == pgtype.TimestampOID || pgType == pgtype.TimestamptzOID
	case transformers.GreenmaskUTCTimestamp:
		return pgType == pgtype.TimestamptzOID || pgType == pgtype.TimestampOID
	default:
		return false
	}
}
