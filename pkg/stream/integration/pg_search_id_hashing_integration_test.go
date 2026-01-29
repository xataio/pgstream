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

func Test_PostgresToSearch_IDHashing(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := testSearchProcessorCfg(store.Config{ElasticsearchURL: elasticsearchURL})
	cfg.Search.Indexer = search.IndexerConfig{HashDocIDs: true}
	runStream(t, ctx, &stream.Config{Listener: testPostgresListenerCfg(), Processor: cfg})

	client, _ := elasticsearch.NewClient(elasticsearchURL)
	longID := strings.Repeat("a", 600)
	testSchema, testTable, testIndex := "id_hash_test", "t", "id_hash_test-1"

	execQuery(t, ctx, fmt.Sprintf("create schema %s", testSchema))
	execQuery(t, ctx, fmt.Sprintf("create table %s.%s(id text primary key, name text)", testSchema, testTable))

	var tableID string
	waitFor(t, ctx, 20*time.Second, func() bool {
		if exists, _ := client.IndexExists(ctx, "pgstream"); !exists {
			return false
		}
		resp := searchSchemaLog(t, ctx, client, testSchema)
		if resp.Hits.Total.Value > 0 {
			tableID = getTablePgstreamID(t, resp.Hits.Hits[0].Source, testTable)
		}
		return tableID != ""
	})

	execQuery(t, ctx, fmt.Sprintf("insert into %s.%s values('%s','x')", testSchema, testTable, longID))

	waitFor(t, ctx, 20*time.Second, func() bool {
		if exists, _ := client.IndexExists(ctx, testIndex); !exists {
			return false
		}
		resp := searchTable(t, ctx, client, testIndex, tableID)
		if resp.Hits.Total.Value != 1 {
			return false
		}
		hash := sha256.Sum256([]byte(longID))
		require.Equal(t, fmt.Sprintf("%s_%s", tableID, hex.EncodeToString(hash[:])), resp.Hits.Hits[0].ID)
		return true
	})
}

func waitFor(t *testing.T, ctx context.Context, timeout time.Duration, condition func() bool) {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-timer.C:
			t.Fatal("timeout waiting for condition")
		case <-ticker.C:
			if condition() {
				return
			}
		}
	}
}
