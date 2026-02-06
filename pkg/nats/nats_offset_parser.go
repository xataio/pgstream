// SPDX-License-Identifier: Apache-2.0

package nats

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type Offset struct {
	Stream   string
	Consumer string
	// StreamSeq is the stream sequence number, analogous to a Kafka offset.
	StreamSeq uint64
}

type OffsetParser interface {
	ToString(o *Offset) string
	FromString(s string) (*Offset, error)
}

type Parser struct{}

var (
	ErrInvalidOffsetFormat = errors.New("invalid format for nats jetstream offset")

	// "/" is used as a separator to concatenate the stream, consumer and
	// stream sequence. The stream sequence is an integer, and the stream
	// and consumer names use [a-zA-Z0-9_-].
	offsetSeparator = "/"
)

func NewOffsetParser() *Parser {
	return &Parser{}
}

func (p *Parser) ToString(o *Offset) string {
	return fmt.Sprintf("%s%s%s%s%d", o.Stream, offsetSeparator, o.Consumer, offsetSeparator, o.StreamSeq)
}

func (p *Parser) FromString(s string) (*Offset, error) {
	parts := strings.Split(s, offsetSeparator)
	if len(parts) != 3 {
		return nil, ErrInvalidOffsetFormat
	}
	stream := parts[0]
	consumer := parts[1]
	streamSeq, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parsing stream sequence from string: %w: %w", ErrInvalidOffsetFormat, err)
	}

	return &Offset{
		Stream:    stream,
		Consumer:  consumer,
		StreamSeq: streamSeq,
	}, nil
}
