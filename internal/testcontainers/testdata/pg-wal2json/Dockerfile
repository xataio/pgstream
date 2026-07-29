# Postgres 17 with the wal2json output plugin.
#
# Logical decoding on a standby requires Postgres 16 or later, and the
# debezium/postgres images used elsewhere in the test suite only ship wal2json
# up to the 14 tag, so the replica tests build their own image instead.
FROM postgres:17

RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql-17-wal2json \
    && rm -rf /var/lib/apt/lists/*
