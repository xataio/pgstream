// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type Offset struct {
	Topic     string
	Partition int
	Offset    int64
}

type OffsetParser interface {
	ToString(o *Offset) string
	FromString(s string) (*Offset, error)
}

type Parser struct{}

var ErrInvalidOffsetFormat = errors.New("invalid format for kafka offset")

func NewOffsetParser() *Parser {
	return &Parser{}
}

func (p *Parser) ToString(o *Offset) string {
	return fmt.Sprintf("%s/%d/%d", o.Topic, o.Partition, o.Offset)
}

func (p *Parser) FromString(s string) (*Offset, error) {
	parts := strings.Split(s, "/")
	if len(parts) != 3 {
		return nil, ErrInvalidOffsetFormat
	}
	topic := parts[0]
	partition, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, fmt.Errorf("parsing partition from string: %w: %w", ErrInvalidOffsetFormat, err)
	}
	offset, err := strconv.Atoi(parts[2])
	if err != nil {
		return nil, fmt.Errorf("parsing offset from string: %w: %w", ErrInvalidOffsetFormat, err)
	}

	return &Offset{
		Topic:     topic,
		Partition: partition,
		Offset:    int64(offset),
	}, nil
}
