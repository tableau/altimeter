#!/bin/bash

export DB_USER=qj
export DB_PASSWORD=password
export DB_NAME=qj
export DB_HOST=127.0.0.1
export SQLALCHEMY_URL="postgres://${DB_USER}:${DB_PASSWORD}@${DB_HOST}/${DB_NAME}"

export PYTHONPATH=".:$PYTHONPATH"

cleanup()
{
  echo "Cleaning up..."
  echo "Killing $docker_container_id"
  docker kill $docker_container_id >/dev/null
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

echo "Waiting for postgres local"
while [ 1 ]; do
    timeout 1 bash -c "cat < /dev/null > /dev/tcp/127.0.0.1/5432"
    if [[ $? -eq 0 ]]; then
        break
    fi
    sleep 5
done
sleep 30
echo "postgres local up"

echo "Creating tables"
alembic -c services/qj/alembic/alembic.ini upgrade head
echo "Running tests"
pytest --ignore=altimeter/qj/alembic/env.py --cov="altimeter" --cov-report=term-missing --cov-fail-under=60 --cov-branch --doctest-modules "altimeter" "tests"
test_status=$?

echo "Killing $docker_container_id"
docker kill $docker_container_id >/dev/null

exit $test_status

