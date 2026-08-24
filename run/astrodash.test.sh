#!/bin/env bash
set -e

cd "$(dirname "$(readlink -f "$0")")"/..

if [[ ! -f "env/.env.dev" ]]; then
  touch env/.env.dev
fi

PROFILE=$1
source run/get_compose_args.sh $PROFILE

# This script execs into an already-running stack; it does not start one. When
# the stack is not up -- or when this helper and `astrodashctl` disagree about
# which project or service to address -- `docker compose exec` reports only
# that a service is not running, which reads as a broken test suite rather than
# a stack that was never started. Say which it is.
if ! docker compose ${COMPOSE_CONFIG} ps --status running --services 2>/dev/null \
     | grep -qx "${TARGET_SERVICE}"; then
  echo "ERROR: service '${TARGET_SERVICE}' is not running in project '${COMPOSE_PROJECT_NAME}'." >&2
  echo "       Start it first:  run/astrodashctl ${PROFILE} up" >&2
  echo "       If it is running, this helper and astrodashctl disagree about the" >&2
  echo "       project name, the compose overlay, or the service name." >&2
  exit 1
fi

set -x
docker compose ${COMPOSE_CONFIG} exec -it ${TARGET_SERVICE} bash -c ' \
  coverage run manage.py test astrodash.tests users.tests -v 2 && \
  coverage report -i --omit=astrodash/tests/*,astrodash/migrations/*,astrodash_project/*,manage.py'
