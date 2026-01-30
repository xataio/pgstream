package integration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/internal/searchstore/elasticsearch"
	"github.com/xataio/pgstream/pkg/stream"
	"github.com/xataio/pgstream/pkg/wal/processor/search"
	"github.com/xataio/pgstream/pkg/wal/processor/search/store"
)

func Test_LongIDsAreHashedBeforeIndexing(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

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
}
