#!/bin/bash

# I need this docker to support both for docker-compose-airflow.yaml (as standalone) and k8s - it will need to set env variable DBT_STANDALONE="false"
if [ -z "${DBT_STANDALONE}" ]; then
  DBT_STANDALONE="true"
fi

echo "dbt is stand alone = $DBT_STANDALONE"

if [ -z "${DBT_PROJECT_DIR}" ]; then
  DBT_PROJECT_DIR="/dbt"
  export DBT_PROJECT_DIR
fi

if [ -z "${DBT_PROFILES_DIR}" ]; then
  DBT_PROFILES_DIR="/dbt"
  export DBT_PROFILES_DIR
fi

echo "$DBT_PROJECT_DIR is project dir and $DBT_PROFILES_DIR is profile dir"

# The rest of the script runs in the main container process, for example,
# to serve dbt docs. It does not block the SSH server.
dbt deps --project-dir /dbt

echo "Finished with deps"

dbt docs generate --project-dir /dbt --profiles-dir /dbt --profile "$CATALOG_TYPE"

echo "Finished generating docs and starting doc server"

if [[ "$DBT_STANDALONE" == "true" ]]; then
  # Start the SSH server to listen for connections
  service ssh start
  echo "SSH Server started"

  sleep 10
  dbt docs serve --project-dir "$DBT_PROJECT_DIR" --profiles-dir "$DBT_PROFILES_DIR" --profile "$CATALOG_TYPE" --host 0.0.0.0 --port 8080
else
    if [ -z "${DBT_PLOG_LEVEL}" ]; then
      DBT_PLOG_LEVEL="info"
    fi

    if [ -z "${DBT_TARGET}" ]; then
      DBT_TARGET="dev"
    fi

    dbt run --profiles-dir "$DBT_PROJECT_DIR" --project-dir "$DBT_PROFILES_DIR"  --profile "$CATALOG_TYPE" --target "$DBT_TARGET" --log-level debug
fi


