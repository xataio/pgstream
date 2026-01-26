// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/internal/searchstore"
	"github.com/xataio/pgstream/internal/searchstore/elasticsearch"
	"github.com/xataio/pgstream/internal/searchstore/opensearch"
	"github.com/xataio/pgstream/pkg/stream"
	"github.com/xataio/pgstream/pkg/wal/processor/search"
	"github.com/xataio/pgstream/pkg/wal/processor/search/store"
)

func Test_PostgresToSearch(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	run := func(t *testing.T, cfg *stream.Config, client searchstore.Client, testSchema string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// create a dedicated schema for the opensearch tests to ensure there's no
		// interference between the other integration tests by having a separate index.
		execQuery(t, ctx, fmt.Sprintf("create schema %s", testSchema))

		runStream(t, ctx, cfg)

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
					testTablePgstreamID = getTablePgstreamID(t, ctx, testSchema, testTable)
					if testTablePgstreamID == "" {
						return false
					}

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
					require.Equal(t, map[string]any{
						"properties": map[string]any{
							"id": map[string]any{
								"type": "alias",
								"path": fmt.Sprintf("%s-1", testTablePgstreamID),
							},
							"name": map[string]any{
								"type": "alias",
								"path": fmt.Sprintf("%s-2", testTablePgstreamID),
							},
						},
					}, mapping[testTable])
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

	t.Run("postgres to opensearch", func(t *testing.T) {
		cfg := &stream.Config{
			Listener: testPostgresListenerCfg(),
			Processor: testSearchProcessorCfg(store.Config{
				OpenSearchURL: opensearchURL,
			}),
		}

		client, err := opensearch.NewClient(opensearchURL)
		require.NoError(t, err)

		run(t, cfg, client, "pg2os_integration_test")
	})

	t.Run("postgres to elasticsearch", func(t *testing.T) {
		cfg := &stream.Config{
			Listener: testPostgresListenerCfg(),
			Processor: testSearchProcessorCfg(store.Config{
				ElasticsearchURL: elasticsearchURL,
			}),
		}

		client, err := elasticsearch.NewClient(elasticsearchURL)
		require.NoError(t, err)

		run(t, cfg, client, "pg2es_integration_test")
	})

	t.Run("long IDs are hashed before indexing", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		cfg := testSearchProcessorCfg(store.Config{ElasticsearchURL: elasticsearchURL})
		cfg.Search.Indexer = search.IndexerConfig{HashDocIDs: true}
		runStream(t, ctx, &stream.Config{Listener: testPostgresListenerCfg(), Processor: cfg})

		client, err := elasticsearch.NewClient(elasticsearchURL)
		require.NoError(t, err)

		testSchema, testTable, testIndex := "id_hash_test", "t", "id_hash_test-1"
		longID := strings.Repeat("a", 600)

		execQuery(t, ctx, fmt.Sprintf("create schema %s", testSchema))
		execQuery(t, ctx, fmt.Sprintf("create table %s.%s(id text primary key, name text)", testSchema, testTable))
		execQuery(t, ctx, fmt.Sprintf("insert into %s.%s values('%s','x')", testSchema, testTable, longID))

		timer := time.NewTimer(20 * time.Second)
		defer timer.Stop()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-timer.C:
				t.Fatal("timeout waiting for indexed document")
			case <-ticker.C:
				exists, err := client.IndexExists(ctx, testIndex)
				require.NoError(t, err)
				if !exists {
					continue
				}
				resp := searchSchemaLog(t, ctx, client, testSchema)
				if resp.Hits.Total.Value == 0 {
					continue
				}
				tableID := getTablePgstreamID(t, resp.Hits.Hits[0].Source, testTable)
				if tableID == "" {
					continue
				}
				resp = searchTable(t, ctx, client, testIndex, tableID)
				if resp.Hits.Total.Value != 1 {
					continue
				}
				hash := sha256.Sum256([]byte(longID))
				expectedID := fmt.Sprintf("%s_%s", tableID, hex.EncodeToString(hash[:]))
				require.Equal(t, expectedID, resp.Hits.Hits[0].ID)
				return
			}
		}
	})
}

func searchTable(t *testing.T, ctx context.Context, client searchstore.Client, index, tableID string) *searchstore.SearchResponse {
	query := searchstore.QueryBody{
		Query: &searchstore.Query{
			Bool: &searchstore.BoolFilter{
				Filter: []searchstore.Condition{
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

func searchQuery(t *testing.T, ctx context.Context, client searchstore.Client, index string, query searchstore.QueryBody, sort *string) *searchstore.SearchResponse {
	queryBytes, err := json.Marshal(&query)
	require.NoError(t, err)

	resp, err := client.Search(ctx, &searchstore.SearchRequest{
		Index: searchstore.Ptr(index),
		Query: bytes.NewBuffer(queryBytes),
		Sort:  sort,
	})
	require.NoError(t, err)

	return resp
}

func getIndexMapping(t *testing.T, ctx context.Context, client searchstore.Client, index string) map[string]any {
	mapping, err := client.GetIndexMappings(ctx, index)
	require.NoError(t, err)

	return mapping.Properties
}

const pgstreamidQuery = `SELECT t.id
FROM pgstream.table_ids t
JOIN pg_class c ON t.oid = c.oid
JOIN pg_namespace n ON c.relnamespace = n.oid
WHERE n.nspname = $1
  AND c.relname = $2`

func getTablePgstreamID(t *testing.T, ctx context.Context, schemaName, tableName string) string {
	conn, err := pglib.NewConn(ctx, pgurl)
	require.NoError(t, err)
	defer conn.Close(ctx)

	pgstreamID := ""
	err = conn.QueryRow(ctx, []any{&pgstreamID}, pgstreamidQuery, schemaName, tableName)
	if err != nil {
		errRelationDoesNotExist := &pglib.ErrRelationDoesNotExist{}
		if errors.Is(err, pglib.ErrNoRows) || errors.As(err, &errRelationDoesNotExist) {
			return ""
		}
	}
	require.NoError(t, err)
	return pgstreamID
}
