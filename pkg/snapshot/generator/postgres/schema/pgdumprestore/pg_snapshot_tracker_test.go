// SPDX-License-Identifier: Apache-2.0

package pgdumprestore

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"

	pglib "github.com/xataio/pgstream/internal/postgres"
	pglibmocks "github.com/xataio/pgstream/internal/postgres/mocks"
	"github.com/xataio/pgstream/internal/progress"
	progressmocks "github.com/xataio/pgstream/internal/progress/mocks"
	synclib "github.com/xataio/pgstream/internal/sync"
)

func TestSnapshotTracker_trackIndexesCreation(t *testing.T) {
	t.Parallel()

	testRows := func(retTuplesDone, retTuplesTotal int64, tableName ...string) *pglibmocks.Rows {
		testTable := "test_table"
		if len(tableName) > 0 {
			testTable = tableName[0]
		}

		return &pglibmocks.Rows{
			NextFn: func(i uint) bool { return i == 1 },
			ScanFn: func(i uint, dest ...any) error {
				require.Len(t, dest, 6)
				table, ok := dest[0].(*string)
				require.True(t, ok)
				index, ok := dest[1].(*string)
				require.True(t, ok)
				phase, ok := dest[2].(*string)
				require.True(t, ok)
				tuplesDone, ok := dest[3].(*int64)
				require.True(t, ok)
				tuplesTotal, ok := dest[4].(*int64)
				require.True(t, ok)
				command, ok := dest[5].(*string)
				require.True(t, ok)

				*table = testTable
				*index = "test_index"
				*phase = "index creation"
				*tuplesDone = retTuplesDone
				*tuplesTotal = retTuplesTotal
				*command = "CREATE INDEX"

				return nil
			},
			CloseFn: func() {},
		}
	}

	tests := []struct {
		name       string
		querier    func(doneChan chan struct{}) pglib.Querier
		barBuilder func(doneChan chan struct{}, _ uint, total int64, description, unit string) *progressmocks.Bar
		tickCount  uint

		wantBarCount           uint
		wantMarkCompletedCalls uint
	}{
		{
			name: "ok - new bar created and completed",
			querier: func(_ chan struct{}) pglib.Querier {
				return &pglibmocks.Querier{
					QueryFn: func(ctx context.Context, _ uint, sql string, args ...any) (pglib.Rows, error) {
						return testRows(100, 100), nil
					},
				}
			},
			barBuilder: func(doneChan chan struct{}, _ uint, total int64, description, unit string) *progressmocks.Bar {
				return &progressmocks.Bar{
					SetCurrentFn: func(i uint32, current int64) error {
						defer func() {
							if i == 1 {
								doneChan <- struct{}{}
							}
						}()
						if i != 1 {
							t.Fatalf("unexpected SetCurrent call %d", i)
						}
						require.Equal(t, int64(100), current)
						return nil
					},
					CurrentFn: func(_ uint32) int64 { return 50 },
					CloseFn:   func() error { return nil },
				}
			},
			tickCount: 1,

			wantBarCount:           1,
			wantMarkCompletedCalls: 1,
		},
		{
			name: "ok - total tuples returned 0 (no bar created)",
			querier: func(doneChan chan struct{}) pglib.Querier {
				return &pglibmocks.Querier{
					QueryFn: func(ctx context.Context, i uint, sql string, args ...any) (pglib.Rows, error) {
						defer func() { doneChan <- struct{}{} }()
						switch i {
						case 1:
							return testRows(0, 0), nil
						default:
							t.Fatalf("unexpected Query call %d", i)
							return nil, nil
						}
					},
				}
			},
			barBuilder: func(doneChan chan struct{}, _ uint, total int64, description, unit string) *progressmocks.Bar {
				t.Fatalf("should not be called")
				return nil
			},
			tickCount: 1,

			wantBarCount:           0,
			wantMarkCompletedCalls: 0,
		},
		{
			name: "ok - no rows returned (no bar created)",
			querier: func(doneChan chan struct{}) pglib.Querier {
				return &pglibmocks.Querier{
					QueryFn: func(ctx context.Context, i uint, sql string, args ...any) (pglib.Rows, error) {
						defer func() { doneChan <- struct{}{} }()
						return nil, errors.New("no rows")
					},
				}
			},
			barBuilder: func(doneChan chan struct{}, _ uint, total int64, description, unit string) *progressmocks.Bar {
				t.Fatalf("should not be called")
				return nil
			},
			tickCount: 1,

			wantBarCount:           0,
			wantMarkCompletedCalls: 0,
		},
		{
			name: "ok - update existing bar",
			querier: func(_ chan struct{}) pglib.Querier {
				return &pglibmocks.Querier{
					QueryFn: func(ctx context.Context, i uint, sql string, args ...any) (pglib.Rows, error) {
						switch i {
						case 1:
							return testRows(50, 100), nil
						case 2:
							return testRows(100, 100), nil
						default:
							t.Fatalf("unexpected Query call %d", i)
							return nil, nil
						}
					},
				}
			},
			barBuilder: func(doneChan chan struct{}, _ uint, total int64, description, unit string) *progressmocks.Bar {
				return &progressmocks.Bar{
					SetCurrentFn: func(i uint32, current int64) error {
						defer func() {
							if i == 2 {
								doneChan <- struct{}{}
							}
						}()
						switch i {
						case 1:
							require.Equal(t, int64(50), current)
						case 2:
							require.Equal(t, int64(100), current)
						default:
							t.Fatalf("unexpected SetCurrent call %d", i)
						}
						return nil
					},
					CurrentFn: func(_ uint32) int64 { return 50 },
				}
			},
			tickCount: 2,

			wantBarCount:           1,
			wantMarkCompletedCalls: 1,
		},
		{
			name: "ok - update existing bar with lower done tuples (new index)",
			querier: func(_ chan struct{}) pglib.Querier {
				return &pglibmocks.Querier{
					QueryFn: func(ctx context.Context, i uint, sql string, args ...any) (pglib.Rows, error) {
						switch i {
						case 1:
							return testRows(50, 100), nil
						case 2:
							return testRows(40, 100), nil
						case 3:
							return testRows(100, 100), nil
						default:
							t.Fatalf("unexpected Query call %d", i)
							return nil, nil
						}
					},
				}
			},
			barBuilder: func(doneChan chan struct{}, buildCount uint, total int64, description, unit string) *progressmocks.Bar {
				switch buildCount {
				case 1:
					return &progressmocks.Bar{
						SetCurrentFn: func(i uint32, current int64) error {
							require.Equal(t, int64(50), current)
							return nil
						},
						CurrentFn: func(_ uint32) int64 { return 50 },
					}
				case 2:
					return &progressmocks.Bar{
						SetCurrentFn: func(i uint32, current int64) error {
							defer func() {
								if i == 2 {
									doneChan <- struct{}{}
								}
							}()
							switch i {
							case 1:
								require.Equal(t, int64(40), current)
							case 2:
								require.Equal(t, int64(100), current)
							default:
								t.Fatalf("unexpected SetCurrent call %d", i)
							}
							return nil
						},
						CurrentFn: func(_ uint32) int64 { return 40 },
					}
				default:
					t.Fatalf("unexpected bar build call %d", buildCount)
					return nil
				}
			},
			tickCount: 3,

			wantBarCount:           2,
			wantMarkCompletedCalls: 2,
		},
		{
			name: "ok - create a new bar for a different table, completing previous one",
			querier: func(_ chan struct{}) pglib.Querier {
				return &pglibmocks.Querier{
					QueryFn: func(ctx context.Context, i uint, sql string, args ...any) (pglib.Rows, error) {
						switch i {
						case 1:
							return testRows(100, 100), nil
						case 2:
							return testRows(100, 100, "another_table"), nil
						default:
							t.Fatalf("unexpected Query call %d", i)
							return nil, nil
						}
					},
				}
			},
			barBuilder: func(doneChan chan struct{}, i uint, total int64, description, unit string) *progressmocks.Bar {
				switch i {
				case 1:
					return &progressmocks.Bar{
						SetCurrentFn: func(i uint32, current int64) error {
							if i != 1 {
								t.Fatalf("unexpected SetCurrent call %d", i)
							}
							require.Equal(t, int64(100), current)
							return nil
						},
						CurrentFn: func(_ uint32) int64 { return 50 },
					}
				case 2:
					return &progressmocks.Bar{
						SetCurrentFn: func(i uint32, current int64) error {
							defer func() {
								doneChan <- struct{}{}
							}()
							if i != 1 {
								t.Fatalf("unexpected SetCurrent call %d", i)
							}
							require.Equal(t, int64(100), current)
							return nil
						},
						CurrentFn: func(_ uint32) int64 { return 50 },
					}
				default:
					t.Fatalf("unexpected bar build call %d", i)
					return nil
				}
			},
			tickCount: 2,

			wantBarCount:           2,
			wantMarkCompletedCalls: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			doneChan := make(chan struct{})
			defer close(doneChan)

			mockBarBuilder := &mockBarBuilder{
				newBarFn: tc.barBuilder,
			}
			barBuilder := func(total int64, description, unit string) progress.Bar {
				return mockBarBuilder.build(doneChan, total, description, unit)
			}

			fakeClock := clockwork.NewFakeClock()

			st := snapshotTracker{
				conn:         tc.querier(doneChan),
				progressBars: synclib.NewMap[string, progress.Bar](),
				barBuilder:   barBuilder,
				clock:        fakeClock,
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go func() {
				st.trackIndexesCreation(ctx)
			}()

			fakeClock.BlockUntilContext(ctx, 1)
			// advance clock to trigger ticker
			for i := uint(0); i < tc.tickCount; i++ {
				fakeClock.Advance(indexProgressCheckInterval)

				// Wait for the ticker to process this tick
				time.Sleep(time.Millisecond * 50)
			}

			select {
			case <-time.After(5 * time.Second):
				t.Fatal("timeout waiting for trackIndexesCreation to finish")
			case <-doneChan:
				cancel()
				// give some time for the Close call to be registered
				time.Sleep(time.Second)
				require.Equal(t, tc.wantBarCount, mockBarBuilder.getBarCount())
				require.Equal(t, tc.wantMarkCompletedCalls, mockBarBuilder.getBarsCompleted())
				return
			}
		})
	}
}

type mockBarBuilder struct {
	bars       []*progressmocks.Bar
	newBarFn   func(doneChan chan struct{}, _ uint, total int64, description, unit string) *progressmocks.Bar
	buildCalls uint32
}

func (mb *mockBarBuilder) build(doneChan chan struct{}, total int64, description, unit string) progress.Bar {
	atomic.AddUint32(&mb.buildCalls, 1)
	if mb.newBarFn != nil {
		bar := mb.newBarFn(doneChan, uint(atomic.LoadUint32(&mb.buildCalls)), total, description, unit)
		mb.bars = append(mb.bars, bar)
		return bar
	}
	return nil
}

func (mb *mockBarBuilder) getBarCount() uint {
	return uint(len(mb.bars))
}

func (mb *mockBarBuilder) getBarsCompleted() uint {
	barsCompleted := uint(0)
	for _, bar := range mb.bars {
		barsCompleted += uint(bar.GetClosedCalls())
	}
	return barsCompleted
}
