#!/bin/bash

set -e

source ./build_images.sh "latest"

docker compose -f docker-compose-airflow.yml "$@"