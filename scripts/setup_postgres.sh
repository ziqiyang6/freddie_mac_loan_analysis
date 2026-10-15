#!/usr/bin/env bash
set -euo pipefail

PG_VERSION="17"
DB_NAME="freddie_mac_risk"
DB_USER="freddie_user"

if ! command -v brew >/dev/null 2>&1; then
  echo "Homebrew is required. Install it first, then rerun this script."
  exit 1
fi

if ! brew list "postgresql@${PG_VERSION}" >/dev/null 2>&1; then
  echo "Installing PostgreSQL ${PG_VERSION}..."
  brew install "postgresql@${PG_VERSION}"
fi

brew services start "postgresql@${PG_VERSION}"

PG_BIN="$(brew --prefix "postgresql@${PG_VERSION}")/bin"
export PATH="$PG_BIN:$PATH"

echo
if psql -d postgres -tAc "SELECT 1 FROM pg_roles WHERE rolname='${DB_USER}'" | grep -q 1; then
  echo "Role ${DB_USER} already exists."
else
  echo "Creating PostgreSQL user ${DB_USER}. You will be prompted to set a password."
  createuser -P "${DB_USER}"
fi

if psql -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='${DB_NAME}'" | grep -q 1; then
  echo "Database ${DB_NAME} already exists."
else
  echo "Creating database ${DB_NAME}..."
  createdb -O "${DB_USER}" "${DB_NAME}"
fi

psql -d "${DB_NAME}" <<SQL
CREATE SCHEMA IF NOT EXISTS risk AUTHORIZATION ${DB_USER};
GRANT ALL ON SCHEMA risk TO ${DB_USER};
SQL

echo
echo "Done."
echo "Database : ${DB_NAME}"
echo "Schema   : risk"
echo "User     : ${DB_USER}"
echo "Host     : localhost"
echo "Port     : 5432"
echo
echo "Test with:"
echo "  psql -h localhost -p 5432 -U ${DB_USER} -d ${DB_NAME}"
