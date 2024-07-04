// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/internal/es"
	"github.com/xataio/pgstream/pkg/schemalog"
	"github.com/xataio/pgstream/pkg/stream"
)

func Test_PostgresToOpensearch(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	cfg := &stream.Config{
		Listener:  testPostgresListenerCfg(),
		Processor: testSearchProcessorCfg(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// create a dedicated schema for the opensearch tests to ensure there's no
	// interference between the other integration tests by having a separate index.
	testSchema := "pg2os_integration_test"
	execQuery(t, ctx, fmt.Sprintf("create schema %s", testSchema))

	// start the configured stream listener/processor
	go func() {
		err := stream.Start(ctx, testLogger(), cfg, nil)
		require.NoError(t, err)
	}()

	client, err := es.NewClient(searchURL)
	require.NoError(t, err)

	var testTablePgstreamID string
	testTable := "test"
	testIndex := fmt.Sprintf("%s-1", testSchema)

	tests := []struct {
		name  string
		query string

		validation func() bool
	}{
		{
			name:  "schema event",
			query: fmt.Sprintf("create table %s.%s(id serial primary key, name text)", testSchema, testTable),

			validation: func() bool {
				resp := searchSchemaLog(t, ctx, client, testSchema)
				if resp.Hits.Total.Value <= 0 {
					return false
				}
				hit := resp.Hits.Hits[0].Source
				testTablePgstreamID = getTablePgstreamID(t, hit, testTable)

				require.Equal(t, false, hit["acked"])
				require.Equal(t, testSchema, hit["schema_name"])
				require.Equal(t, float64(2), hit["version"])

				mapping := getIndexMapping(t, ctx, client, testIndex)
				require.Equal(t, map[string]any{"type": "keyword"}, mapping["_table"])
				require.Equal(t, map[string]any{"type": "long"}, mapping[fmt.Sprintf("%s-1", testTablePgstreamID)])
				require.Equal(t, map[string]any{
					"fields": map[string]any{
						"text": map[string]any{"type": "text"},
					},
					"ignore_above": float64(32766),
					"type":         "keyword",
				}, mapping[fmt.Sprintf("%s-2", testTablePgstreamID)])
				return true
			},
		},
		{
			name:  "data event",
			query: fmt.Sprintf("insert into %s.%s(name) values('a')", testSchema, testTable),

			validation: func() bool {
				resp := searchTable(t, ctx, client, testIndex, testTablePgstreamID)
				if resp.Hits.Total.Value != 1 {
					return false
				}
				hit := resp.Hits.Hits[0]
				t.Log(hit)
				require.Equal(t, fmt.Sprintf("%s_1", testTablePgstreamID), hit.ID)
				require.Equal(t, "a", hit.Source[fmt.Sprintf("%s-2", testTablePgstreamID)])
				return true
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			execQuery(t, ctx, tc.query)

			timer := time.NewTimer(20 * time.Second)
			defer timer.Stop()
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-timer.C:
					cancel()
					t.Error("timeout waiting for opensearch data")
					return
				case <-ticker.C:
					exists, err := client.IndexExists(ctx, testIndex)
					require.NoError(t, err)
					if exists && tc.validation() {
						return
					}
				}
			}
		})
	}
}

func searchSchemaLog(t *testing.T, ctx context.Context, client *es.Client, schemaName string) *es.SearchResponse {
	query := es.QueryBody{
		Query: &es.Query{
			Bool: &es.BoolFilter{
				Filter: []es.Condition{
					{
						Term: map[string]any{
							"schema_name": schemaName,
						},
					},
				},
			},
		},
	}

	return searchQuery(t, ctx, client, "pgstream", query, es.Ptr("version:desc"))
}

func searchTable(t *testing.T, ctx context.Context, client *es.Client, index, tableID string) *es.SearchResponse {
	query := es.QueryBody{
		Query: &es.Query{
			Bool: &es.BoolFilter{
				Filter: []es.Condition{
					{
						Term: map[string]any{
							"_table": tableID,
						},
					},
				},
			},
		},
	}

	return searchQuery(t, ctx, client, index, query, nil)
}

func searchQuery(t *testing.T, ctx context.Context, client *es.Client, index string, query es.QueryBody, sort *string) *es.SearchResponse {
	queryBytes, err := json.Marshal(&query)
	require.NoError(t, err)

	resp, err := client.Search(ctx, &es.SearchRequest{
		Index: es.Ptr(index),
		Query: bytes.NewBuffer(queryBytes),
		Sort:  sort,
	})
	require.NoError(t, err)

	return resp
}

func getIndexMapping(t *testing.T, ctx context.Context, client *es.Client, index string) map[string]any {
	mapping, err := client.GetIndexMappings(ctx, index)
	require.NoError(t, err)

	return mapping.Properties
}

func getTablePgstreamID(t *testing.T, source map[string]any, tableName string) string {
	sourceBytes, err := json.Marshal(source)
	require.NoError(t, err)

	schemaLog := &schemalog.LogEntry{}
	err = json.Unmarshal(sourceBytes, schemaLog)
	require.NoError(t, err)

	if len(schemaLog.Schema.Tables) != 1 {
		return ""
	}

	for _, table := range schemaLog.Schema.Tables {
		if table.Name == tableName {
			return table.PgstreamID
		}
	}

	return ""
}
