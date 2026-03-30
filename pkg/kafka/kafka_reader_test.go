// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	loglib "github.com/xataio/pgstream/pkg/log"
)

// fakeReader simulates a kafka.Reader. When hangCount > 0, the first
// hangCount calls to FetchMessage block until the context is cancelled
// (reproducing the kafka-go empty-partition-assignment bug). Subsequent
// calls return a message immediately.
type fakeReader struct {
	calls      atomic.Int32
	hangCount  int
	closeErr   error
	closeCalls atomic.Int32
}

func (f *fakeReader) FetchMessage(ctx context.Context) (kafkago.Message, error) {
	call := int(f.calls.Add(1))
	if call <= f.hangCount {
		<-ctx.Done()
		return kafkago.Message{}, ctx.Err()
	}
	return kafkago.Message{
		Topic:     "test-topic",
		Partition: 0,
		Offset:    42,
		Value:     []byte("hello"),
	}, nil
}

func (f *fakeReader) CommitMessages(_ context.Context, _ ...kafkago.Message) error {
	return nil
}

func (f *fakeReader) Close() error {
	f.closeCalls.Add(1)
	return f.closeErr
}

// fakeReaderWithError returns a real error (not a context error) on
// every call, simulating a broker/auth/network failure.
type fakeReaderWithError struct {
	err error
}

func (f *fakeReaderWithError) FetchMessage(_ context.Context) (kafkago.Message, error) {
	return kafkago.Message{}, f.err
}

func (f *fakeReaderWithError) CommitMessages(_ context.Context, _ ...kafkago.Message) error {
	return nil
}

func (f *fakeReaderWithError) Close() error {
	return nil
}

func TestFetchMessage_FirstFetchHangsForever(t *testing.T) {
	// This test reproduces the kafka-go bug: FetchMessage blocks
	// indefinitely when the consumer gets an empty partition assignment.
	// Without the retry mechanism, FetchMessage never returns.
	//
	// The fake hangs 3 times (blocks until context deadline), then
	// returns a message on the 4th call — simulating a fresh consumer
	// group join that gets the partition assigned correctly.

	fake := &fakeReader{hangCount: 3}

	r := &Reader{
		reader:     fake,
		newReader:  func() kafkaFetcher { return fake },
		logger:     loglib.NewNoopLogger(),
		firstFetch: true,
	}

	// Allow enough time for 3 retry cycles (3 × firstFetchTimeout) plus margin.
	timeout := time.Duration(4) * firstFetchTimeout
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	msg, err := r.FetchMessage(ctx)
	if err != nil {
		t.Fatalf("expected message after retry, got error: %v", err)
	}
	if string(msg.Value) != "hello" {
		t.Fatalf("expected message value 'hello', got %q", string(msg.Value))
	}
	if int(fake.calls.Load()) != 4 {
		t.Fatalf("expected 4 FetchMessage calls (3 hangs + 1 success), got %d", fake.calls.Load())
	}
	if int(fake.closeCalls.Load()) != 3 {
		t.Fatalf("expected 3 Close calls (one per retry), got %d", fake.closeCalls.Load())
	}
}

func TestFetchMessage_FirstFetchExhaustsAllAttempts(t *testing.T) {
	// When the caller's context expires while retries are in progress,
	// FetchMessage should return the context error.

	fake := &fakeReader{hangCount: 100} // always hangs

	r := &Reader{
		reader:     fake,
		newReader:  func() kafkaFetcher { return fake },
		logger:     loglib.NewNoopLogger(),
		firstFetch: true,
	}

	// Use a timeout shorter than firstFetchTimeout × firstFetchAttempts
	// so the caller's context expires during retries.
	ctx, cancel := context.WithTimeout(context.Background(), firstFetchTimeout+2*time.Second)
	defer cancel()

	_, err := r.FetchMessage(ctx)
	if err == nil {
		t.Fatal("expected error when context expires, got nil")
	}
}

func TestFetchMessage_PropagatesRealErrors(t *testing.T) {
	// Non-timeout errors (broker down, auth failure, etc.) should
	// propagate immediately without retrying.

	brokerErr := fmt.Errorf("broker authentication failed")
	fake := &fakeReaderWithError{err: brokerErr}

	r := &Reader{
		reader:     fake,
		newReader:  func() kafkaFetcher { return fake },
		logger:     loglib.NewNoopLogger(),
		firstFetch: true,
	}

	_, err := r.FetchMessage(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestFetchMessage_SucceedsImmediately(t *testing.T) {
	// When the first FetchMessage succeeds (no bug), no retry needed.

	fake := &fakeReader{hangCount: 0}

	r := &Reader{
		reader:     fake,
		newReader:  func() kafkaFetcher { return fake },
		logger:     loglib.NewNoopLogger(),
		firstFetch: true,
	}

	msg, err := r.FetchMessage(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(msg.Value) != "hello" {
		t.Fatalf("expected 'hello', got %q", string(msg.Value))
	}
	if int(fake.calls.Load()) != 1 {
		t.Fatalf("expected 1 FetchMessage call, got %d", fake.calls.Load())
	}
	if int(fake.closeCalls.Load()) != 0 {
		t.Fatalf("expected 0 Close calls, got %d", fake.closeCalls.Load())
	}
}
