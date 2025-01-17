// SPDX-License-Identifier: Apache-2.0

package searchstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/mitchellh/mapstructure"
)

type ResponseError struct {
	Type         string           `mapstructure:"type"`
	Reason       string           `mapstructure:"reason"`
	FailedShards []map[string]any `mapstructure:"failed_shards"`
	CausedBy     *CausedBy        `mapstructure:"caused_by"`
	RootCause    []RootCause      `mapstructure:"root_cause"`
}

type CausedBy struct {
	Type       string `mapstructure:"type"`
	Reason     string `mapstructure:"reason"`
	MaxBuckets int    `mapstructure:"max_buckets"`
}

type RootCause struct {
	Type   string `mapstructure:"type"`
	Reason string `mapstructure:"reason"`
}

type RetryableError struct {
	Cause error
}

func (r RetryableError) Error() string {
	return fmt.Sprintf("%v", r.Cause)
}

func (r RetryableError) Unwrap() error {
	return r.Cause
}

type ErrIllegalArgument struct {
	Reason string
}

func (e *ErrIllegalArgument) Error() string {
	return e.Reason
}

type ErrResourceAlreadyExists struct {
	Reason string
}

func (e ErrResourceAlreadyExists) Error() string {
	return fmt.Sprintf("resource already exists: %s", e.Reason)
}

type ErrQueryInvalid struct {
	Cause error
}

func (e ErrQueryInvalid) Error() string {
	return e.Cause.Error()
}

const (
	SearchExecutionException       = "search_phase_execution_exception"
	TooManyBucketsException        = "too_many_buckets_exception"
	TooManyNestedClausesException  = "too_many_nested_clauses"
	TooManyClausesException        = "too_many_clauses"
	IllegalArgumentException       = "illegal_argument_exception"
	SnapshotInProgressException    = "snapshot_in_progress_exception"
	ResourceAlreadyExistsException = "resource_already_exists_exception"
)

var (
	ErrTooManyRequests            = errors.New("too many requests")
	ErrTooManyBuckets             = errors.New("too many buckets")
	ErrTooManyNestedClauses       = errors.New("too many nested clauses")
	ErrTooManyClauses             = errors.New("too many clauses")
	ErrUnsupportedSearchFieldType = errors.New("unsupported search field type")
	ErrResourceNotFound           = errors.New("search resource not found")
)

type apiResponse interface {
	GetBody() io.ReadCloser
	GetStatusCode() int
	IsError() bool
}

func IsErrResponse(res apiResponse) error {
	if res.IsError() {
		if res.GetStatusCode() == http.StatusNotFound {
			return fmt.Errorf("%w: %w", ErrResourceNotFound, ExtractResponseError(res.GetBody(), res.GetStatusCode()))
		}
		return ExtractResponseError(res.GetBody(), res.GetStatusCode())
	}

	return nil
}

func ExtractResponseError(body io.ReadCloser, statusCode int) error {
	var e map[string]any
	if err := json.NewDecoder(body).Decode(&e); err != nil {
		return fmt.Errorf("decoding error response: %w", err)
	}

	var errType, errReason any
	if eErr, ok := e["error"]; ok {
		var esError ResponseError
		err := mapstructure.Decode(eErr, &esError)
		if err != nil {
			errType = "<unknown error type>"
			errReason = "<unknown error reason>"
		} else {
			errType = esError.Type
			errReason = esError.Reason
			if esError.Type == SearchExecutionException {
				marshalled, _ := json.Marshal(esError.FailedShards)
				errReason = string(marshalled)

				if esError.CausedBy != nil {
					if esError.CausedBy.Type == TooManyBucketsException {
						return ErrTooManyBuckets
					}
					if esError.CausedBy.Type == TooManyNestedClausesException {
						return ErrTooManyNestedClauses
					}
					if esError.CausedBy.Type == TooManyClausesException {
						return ErrTooManyClauses
					}
					if esError.CausedBy.Type == IllegalArgumentException {
						return &ErrIllegalArgument{Reason: esError.CausedBy.Reason}
					}
				}
				if len(esError.RootCause) > 0 {
					if esError.RootCause[0].Type == TooManyNestedClausesException {
						return ErrTooManyNestedClauses
					}
					if strings.Contains(esError.RootCause[0].Reason, "function score query returned an invalid score") {
						return &ErrIllegalArgument{Reason: esError.RootCause[0].Reason}
					}
				}
			}
		}
	}

	if err, ok := getRetryableError(statusCode); ok {
		return RetryableError{Cause: err}
	}

	if statusCode == http.StatusNotFound {
		return fmt.Errorf("%w: [%d]: %s: %s", ErrResourceNotFound, statusCode, errType, errReason)
	}

	if statusCode == http.StatusBadRequest {
		switch errType {
		case ResourceAlreadyExistsException:
			reason, _ := errReason.(string)
			return ErrResourceAlreadyExists{Reason: reason}
		case SnapshotInProgressException:
			return RetryableError{Cause: fmt.Errorf("[%d] %s: %s", statusCode, errType, errReason)}
		default:
			// Generic bad request
			return ErrQueryInvalid{
				Cause: fmt.Errorf("%v", errReason),
			}
		}
	}

	return fmt.Errorf("[%d] %s: %s", statusCode, errType, errReason)
}

func getRetryableError(statusCode int) (error, bool) {
	switch statusCode {
	case http.StatusRequestTimeout:
		return errors.New("request timeout"), true
	case http.StatusLocked:
		return errors.New("resource locked"), true
	case http.StatusTooEarly:
		return errors.New("too early"), true
	case http.StatusTooManyRequests:
		return ErrTooManyRequests, true
	case http.StatusBadGateway:
		return errors.New("bad gateway"), true
	case http.StatusServiceUnavailable:
		return errors.New("service unavailable"), true
	case http.StatusGatewayTimeout:
		return errors.New("gateway timeout"), true
	}

	return nil, false
}
