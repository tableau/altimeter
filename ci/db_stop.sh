#!/bin/bash

set -ef -o pipefail

if [[ -f postgres_docker_container.id ]]; then
    docker kill $(cat postgres_docker_container.id)
    rm -f postgres_docker_container.id
fi
