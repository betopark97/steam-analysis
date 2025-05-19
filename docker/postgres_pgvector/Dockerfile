# Dockerfile for PostgreSQL 17 + pgvector
FROM postgres:17.5

# Install tools needed to build pgvector
RUN apt-get update && apt-get install -y \
    git \
    build-essential \
    postgresql-server-dev-17

# Clone and install pgvector from source
RUN git clone --branch v0.8.0 https://github.com/pgvector/pgvector.git \
    && cd pgvector && make && make install \
    && cd .. && rm -rf pgvector

# Clean up to reduce image size
RUN apt-get remove --purge -y \
    git \
    build-essential \
    postgresql-server-dev-17 \
    && apt-get autoremove -y && apt-get clean