// SPDX-License-Identifier: Apache-2.0

package testcontainers

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/testcontainers/testcontainers-go/wait"
)

// SASLUser is the set of credentials the SASL enabled broker accepts.
type SASLUser struct {
	Username string
	Password string
}

// saslMechanisms are the mechanisms the SASL enabled broker offers. They match
// the mechanisms supported by the kafka package.
var saslMechanisms = []string{"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"}

const (
	jaasConfigPath = "/etc/kafka/kafka_server_jaas.conf"
	// the broker only reads the SCRAM credentials from the cluster metadata, so
	// the login module needs no options. The PLAIN credentials, in contrast,
	// are part of this file.
	jaasConfigTemplate = `KafkaServer {
   org.apache.kafka.common.security.plain.PlainLoginModule required
   username=%[1]q
   password=%[2]q
   user_%[1]s=%[2]q;
   org.apache.kafka.common.security.scram.ScramLoginModule required;
};
`
)

// SetupKafkaSASLContainer starts a Kafka broker that requires SASL
// authentication on the listener the tests connect to, and accepts the given
// user over PLAIN, SCRAM-SHA-256 and SCRAM-SHA-512. One broker therefore covers
// every mechanism, and a test selects one by configuring the client.
//
// The module names the external listener PLAINTEXT and writes that name into
// the advertised listeners, so the name stays while the security protocol
// behind it becomes SASL_PLAINTEXT. The inter broker and controller listeners
// stay unauthenticated: the broker only has to authenticate the clients under
// test.
func SetupKafkaSASLContainer(ctx context.Context, brokers *[]string, user SASLUser) (cleanup, error) {
	jaasConfig := fmt.Sprintf(jaasConfigTemplate, user.Username, user.Password)

	opts := []testcontainers.ContainerCustomizer{
		kafka.WithClusterID("test-sasl-cluster"),
		testcontainers.WithFiles(testcontainers.ContainerFile{
			Reader:            strings.NewReader(jaasConfig),
			ContainerFilePath: jaasConfigPath,
			FileMode:          0o644,
		}),
		testcontainers.WithEnv(map[string]string{
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":                  "BROKER:PLAINTEXT,PLAINTEXT:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT",
			"KAFKA_SASL_ENABLED_MECHANISMS":                         strings.Join(saslMechanisms, ","),
			"KAFKA_LISTENER_NAME_PLAINTEXT_SASL_ENABLED_MECHANISMS": strings.Join(saslMechanisms, ","),
			// the JAAS configuration has to arrive as a file: the confluent
			// images turn every KAFKA_ prefixed variable into a broker
			// property by splitting the variable on the first "=", which drops
			// any value that contains one, and a JAAS configuration is all
			// "key=value" options.
			"KAFKA_OPTS": "-Djava.security.auth.login.config=" + jaasConfigPath,
			// the readiness check the image runs before it starts the REST
			// proxy has no credentials, so point it at the unauthenticated
			// inter broker listener. Left on the default it checks the SASL
			// listener, fails, and takes the container down with it.
			"KAFKA_REST_BOOTSTRAP_SERVERS": "BROKER://0.0.0.0:9092",
		}),
		testcontainers.WithWaitStrategy(
			wait.ForLog("Kafka Server started").
				WithOccurrence(1).
				WithStartupTimeout(60 * time.Second),
		),
	}

	ctr, err := kafka.Run(ctx, kafkaImage, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to start SASL kafka container: %w", err)
	}

	terminate := func() error {
		return ctr.Terminate(ctx)
	}

	// The SCRAM credentials are not broker configuration: they live in the
	// cluster metadata and have to be written once the broker answers. The
	// inter broker listener is unauthenticated, so this runs without
	// credentials from inside the container.
	if err := addSCRAMCredentials(ctx, ctr, user); err != nil {
		return nil, withTerminate(err, terminate)
	}

	*brokers, err = ctr.Brokers(ctx)
	if err != nil {
		return nil, withTerminate(fmt.Errorf("retrieving brokers for SASL kafka container: %w", err), terminate)
	}

	return terminate, nil
}

func addSCRAMCredentials(ctx context.Context, ctr testcontainers.Container, user SASLUser) error {
	for _, mechanism := range saslMechanisms {
		if !strings.HasPrefix(mechanism, "SCRAM") {
			continue
		}

		exitCode, output, err := ctr.Exec(ctx, []string{
			"kafka-configs",
			"--bootstrap-server", "localhost:9092",
			"--alter",
			"--add-config", fmt.Sprintf("%s=[password=%s]", mechanism, user.Password),
			"--entity-type", "users",
			"--entity-name", user.Username,
		})
		if err != nil {
			return fmt.Errorf("adding %s credentials: %w", mechanism, err)
		}
		if exitCode != 0 {
			return fmt.Errorf("adding %s credentials: exit code %d: %s", mechanism, exitCode, readAll(output))
		}
	}

	return nil
}

// withTerminate reports the failure that made the caller give up on the
// container, keeping any failure to remove it as context rather than losing
// either one.
func withTerminate(err error, terminate cleanup) error {
	if terminateErr := terminate(); terminateErr != nil {
		return fmt.Errorf("%w (terminating container: %w)", err, terminateErr)
	}
	return err
}

func readAll(r io.Reader) string {
	if r == nil {
		return ""
	}
	output, err := io.ReadAll(r)
	if err != nil {
		return ""
	}
	return string(output)
}
