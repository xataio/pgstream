FROM postgres:17.7

# Install wal2json and build dependencies for anonymizer
RUN apt-get update \
	&& apt-get install -y \
	postgresql-17-wal2json \
	postgresql-server-dev-17 \
	git \
	make \
	gcc \
	&& rm -rf /var/lib/apt/lists/*

# Install PostgreSQL Anonymizer from source
RUN cd /tmp \
	&& git clone --depth 1 --branch 1.3.2 https://gitlab.com/dalibo/postgresql_anonymizer.git \
	&& cd postgresql_anonymizer \
	&& make extension \
	&& make install \
	&& cd / \
	&& rm -rf /tmp/postgresql_anonymizer
