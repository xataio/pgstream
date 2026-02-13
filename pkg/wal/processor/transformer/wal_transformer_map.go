// SPDX-License-Identifier: Apache-2.0

package transformer

import (
	"errors"
	"maps"

	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/transformers"
)

// TransformerMap holds the active and noop transformers for each table/column
// combination. It is not concurrency safe.
type TransformerMap struct {
	activeTransformerMap map[string]ColumnTransformers
	noopTransformerMap   map[string]ColumnTransformers
}

// NewTransformerMap initializes and returns a new instance of TransformerMap.
func NewTransformerMap() *TransformerMap {
	return &TransformerMap{
		activeTransformerMap: make(map[string]ColumnTransformers),
		noopTransformerMap:   make(map[string]ColumnTransformers),
	}
}

// Close closes all active transformers in the map and aggregates and returns
// any errors that occur during closing.
func (tm *TransformerMap) Close() error {
	var closeErrs error
	for _, transformer := range tm.activeTransformerMap {
		for _, colTransformer := range transformer {
			if colTransformer == nil {
				continue
			}
			if err := colTransformer.Close(); err != nil {
				closeErrs = errors.Join(closeErrs, err)
			}
		}
	}
	return closeErrs
}

// AddActiveTransformer adds an active transformer for the specified schema, table, and column.
func (tm *TransformerMap) AddActiveTransformer(schema, table, column string, transformer transformers.Transformer) {
	key := schemaTableKey(schema, table)
	if _, found := tm.activeTransformerMap[key]; !found {
		tm.activeTransformerMap[key] = make(map[string]transformers.Transformer)
	}
	tm.activeTransformerMap[key][column] = transformer
}

// AddNoopTransformer adds a noop transformer for the specified schema, table, and column.
func (tm *TransformerMap) AddNoopTransformer(schema, table, column string) {
	key := schemaTableKey(schema, table)
	if _, found := tm.noopTransformerMap[key]; !found {
		tm.noopTransformerMap[key] = make(map[string]transformers.Transformer)
	}
	tm.noopTransformerMap[key][column] = nil
}

// GetActiveColumnTransformers retrieves the active column transformers for the
// specified schema and table. Does not include noop transformers.
func (tm *TransformerMap) GetActiveColumnTransformers(schema, table string) (ColumnTransformers, bool) {
	key := schemaTableKey(schema, table)
	transformers, found := tm.activeTransformerMap[key]
	return transformers, found
}

// GetNoopColumnTransformers retrieves the noop column transformers for the
// specified schema and table. Does not include active transformers.
func (tm *TransformerMap) GetNoopColumnTransformers(schema, table string) (ColumnTransformers, bool) {
	key := schemaTableKey(schema, table)
	transformers, found := tm.noopTransformerMap[key]
	return transformers, found
}

// GetAllColumnTransformers retrieves all column transformers (active and noop)
// for the specified schema and table. If no transformers are found, it returns
// false.
func (tm *TransformerMap) GetAllColumnTransformers(schema, table string) (ColumnTransformers, bool) {
	key := schemaTableKey(schema, table)
	activeTransformers, activeFound := tm.activeTransformerMap[key]
	noopTransformers, noopFound := tm.noopTransformerMap[key]

	if !activeFound && !noopFound {
		return nil, false
	}

	allTransformers := make(ColumnTransformers, len(activeTransformers)+len(noopTransformers))
	maps.Copy(allTransformers, activeTransformers)
	maps.Copy(allTransformers, noopTransformers)

	return allTransformers, true
}

func schemaTableKey(schema, table string) string {
	return pglib.QuoteQualifiedIdentifier(schema, table)
}
