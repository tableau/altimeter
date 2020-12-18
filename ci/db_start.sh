#!/bin/bash

set -ef -o pipefail

rm -f postgres_docker_container.id

if [[ "$PWD" != *travis* ]]; then
    echo "Running in local mode"

    cleanup()
    {
      echo "Cleaning up..."
      echo "Killing $docker_container_id"
      docker kill $docker_container_id >/dev/null
      rm -f postgres_docker_container.id
      exit 1
    }

    trap cleanup INT

    docker_container_id=$(docker run \
                -d \
                -p 5432:5432 \
                -e POSTGRES_USER=$DB_USER \
                -e POSTGRES_PASSWORD=$DB_PASSWORD \
                -e POSTGRES_DB=$DB_NAME \
                postgres:10.7)

    echo "Started postgres @ $docker_container_id"
    echo $docker_container_id > postgres_docker_container.id

    echo "Waiting for postgres local"
    set +e
    while [ 1 ]; do
        timeout 1 bash -c "cat < /dev/null > /dev/tcp/127.0.0.1/5432"
        if [[ $? -eq 0 ]]; then
            break
        fi
        sleep 5
    done
    set -e
    sleep 30
    echo "postgres local up"
else
    echo "Running in travis-ci mode"
    psql -c "create database $DB_NAME;" -U $DB_USER
fi

echo "Creating tables"
export SQLALCHEMY_URL="postgres://${DB_USER}:${DB_PASSWORD}@${DB_HOST}/${DB_NAME}"
alembic -c services/qj/alembic/alembic.ini upgrade head
