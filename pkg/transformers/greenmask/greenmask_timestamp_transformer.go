// SPDX-License-Identifier: Apache-2.0

package greenmask

import (
	"context"
	"errors"
	"fmt"
	"time"

	greenmasktransformers "github.com/eminano/greenmask/pkg/generators/transformers"
	"github.com/xataio/pgstream/pkg/transformers"
)

type UTCTimestampTransformer struct {
	transformer *greenmasktransformers.Timestamp
}

var (
	errMinMaxTimestampNotSpecified = errors.New("greenmask_timestamp: min_timestamp and max_timestamp must be specified")
	errInvalidTimestamp            = errors.New("greenmask_timestamp: min_timestamp and max_timestamp must be valid RFC3339 timestamps")
)

var UTCTimestampTransformerParams = []string{"truncate_part", "min_timestamp", "max_timestamp", "generator"}

func NewUTCTimestampTransformer(params transformers.Parameters) (*UTCTimestampTransformer, error) {
	if err := transformers.ValidateParameters(params, UTCTimestampTransformerParams); err != nil {
		return nil, err
	}

	truncatePart, err := findParameter(params, "truncate_part", "")
	if err != nil {
		return nil, fmt.Errorf("greenmask_utc_timestamp: truncate_part must be a string: %w", err)
	}
	minDateStr, err := findParameter(params, "min_timestamp", "")
	if err != nil {
		return nil, fmt.Errorf("greenmask_utc_timestamp: min_timestamp must be a string: %w", err)
	}
	maxDateStr, err := findParameter(params, "max_timestamp", "")
	if err != nil {
		return nil, fmt.Errorf("greenmask_utc_timestamp: max_timestamp must be a string: %w", err)
	}
	if minDateStr == "" || maxDateStr == "" {
		return nil, errMinMaxTimestampNotSpecified
	}
	minDate, err := time.Parse(time.RFC3339, minDateStr)
	if err != nil {
		return nil, errInvalidTimestamp
	}
	minDate = minDate.UTC()

	maxDate, err := time.Parse(time.RFC3339, maxDateStr)
	if err != nil {
		return nil, errInvalidTimestamp
	}
	maxDate = maxDate.UTC()

	limiter, err := greenmasktransformers.NewTimestampLimiter(minDate, maxDate)
	if err != nil {
		return nil, err
	}

	t, err := greenmasktransformers.NewRandomTimestamp(truncatePart, limiter)
	if err != nil {
		return nil, err
	}

	if err := setGenerator(t, params); err != nil {
		return nil, err
	}

	return &UTCTimestampTransformer{
		transformer: t,
	}, nil
}

func (t *UTCTimestampTransformer) Transform(_ context.Context, value transformers.Value) (any, error) {
	var toTransform []byte
	switch val := value.TransformValue.(type) {
	case time.Time:
		toTransform = []byte(val.Format(time.RFC3339))
	case []byte:
		toTransform = val
	case string:
		toTransform = []byte(val)
	default:
		return nil, transformers.ErrUnsupportedValueType
	}
	return t.transformer.Transform(nil, toTransform)
}

func (t *UTCTimestampTransformer) CompatibleTypes() []transformers.SupportedDataType {
	return []transformers.SupportedDataType{
		transformers.DatetimeDataType,
		transformers.ByteArrayDataType,
		transformers.StringDataType,
	}
}

func (t *UTCTimestampTransformer) Type() transformers.TransformerType {
	return transformers.GreenmaskUTCTimestamp
}
