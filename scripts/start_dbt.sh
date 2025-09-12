#!/bin/bash

if [ -z "${DBT_PROJECT_DIR}" ]; then
  DBT_PROJECT_DIR="/dbt"
  export DBT_PROJECT_DIR
fi

if [ -z "${DBT_PROFILES_DIR}" ]; then
  DBT_PROFILES_DIR="/dbt"
  export DBT_PROFILES_DIR
fi

echo "$DBT_PROJECT_DIR is project dir and $DBT_PROFILES_DIR is profile dir"

# Start the SSH server to listen for connections
service ssh start

echo "SSH Server started"

sleep 10

# The rest of the script runs in the main container process, for example,
# to serve dbt docs. It does not block the SSH server.
dbt deps --project-dir /dbt

echo "Finished with deps"

dbt docs generate --project-dir /dbt --profiles-dir /dbt --profile "$CATALOG_TYPE"

echo "Finished generating docs and starting doc server"

dbt docs serve --project-dir /dbt --profiles-dir /dbt --profile "$CATALOG_TYPE" --host 0.0.0.0 --port 8080
