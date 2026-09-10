// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"errors"
	"fmt"
	"strings"

	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
)

const (
	saslPlain       = "plain"
	saslScramSHA256 = "scram-sha-256"
	saslScramSHA512 = "scram-sha-512"
)

var (
	errMissingSASLMechanism = fmt.Errorf("kafka SASL mechanism is required, must be one of [%s, %s, %s]", saslPlain, saslScramSHA256, saslScramSHA512)
	errMissingSASLUsername  = errors.New("kafka SASL username is required")
	errMissingSASLPassword  = errors.New("kafka SASL password is required")
)

// mechanism returns the SASL mechanism for the configuration. It returns a nil
// mechanism if SASL is not enabled, which disables the SASL authentication.
func (c *SASLConfig) mechanism() (sasl.Mechanism, error) {
	if !c.Enabled {
		return nil, nil
	}

	if c.Username == "" {
		return nil, errMissingSASLUsername
	}
	if c.Password == "" {
		return nil, errMissingSASLPassword
	}

	switch strings.ToLower(c.Mechanism) {
	case "":
		return nil, errMissingSASLMechanism
	case saslPlain:
		return plain.Mechanism{Username: c.Username, Password: c.Password}, nil
	case saslScramSHA256:
		return scram.Mechanism(scram.SHA256, c.Username, c.Password)
	case saslScramSHA512:
		return scram.Mechanism(scram.SHA512, c.Username, c.Password)
	default:
		return nil, fmt.Errorf("unsupported SASL mechanism [%s], must be one of [%s, %s, %s]", c.Mechanism, saslPlain, saslScramSHA256, saslScramSHA512)
	}
}
