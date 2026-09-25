#!/bin/env bash
set -e

PROFILE=$1
PURGE_OPTION=$2

ENV_FILE="env/.env.dev"
# The compose project defines one application service, named `app`, on every
# profile. This is not a per-profile value and must not become one again: it
# was `app_dev` here for profiles whose service has never had that name, so
# every `exec` this helper produced failed with "service is not running".
TARGET_SERVICE="app"
# The per-profile overlay `astrodashctl` layers over the base compose file.
# Without it the dev bind-mount, the dev-only services, and the profile's own
# container names are all absent, so this helper described a different stack
# from the one `astrodashctl up` actually starts.
OVERLAY_FILE="docker-compose.dev.yaml"
# Project names must match `astrodashctl` exactly, or this helper addresses a
# project that has nothing running in it. A pre-set value still wins, so a
# caller can point at a project of their own.
PROJECT_NAME="astrodash-dev"
COMPOSE_ARGS=""
case "$PROFILE" in
  "")
    echo "ERROR: You must specify a profile (e.g. $0 slim_dev)"
    exit 1
    ;;
  ci)
    ENV_FILE="env/.env.ci"
    OVERLAY_FILE="docker-compose.ci.yaml"
    PROJECT_NAME="astrodash-ci"
    COMPOSE_ARGS="--build --exit-code-from app"
    ;;
  slim_dev)
    COMPOSE_ARGS="--build --abort-on-container-exit"
    ;;
  slim_prod | full_prod)
    ENV_FILE="env/.env.prod"
    OVERLAY_FILE="docker-compose.prod.yaml"
    PROJECT_NAME="astrodash-prod"
    ;;
  docs)
    OVERLAY_FILE="docker-compose.docs.yaml"
    ;;
  *)
    COMPOSE_ARGS="--build"
    ;;
esac
COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-$PROJECT_NAME}"

PURGE_VOLUMES=""
case "${PURGE_OPTION}" in
  "")
    echo "Retaining all data volumes."
    ;;
  "--purge-all")
    COMPOSE_ARGS="--volumes"
    echo "Purging all data volumes..."
  ;;
  "--purge-db")
    COMPOSE_ARGS=""
    PURGE_VOLUMES="${COMPOSE_PROJECT_NAME}_astrodash-db ${COMPOSE_PROJECT_NAME}_django-static"
    echo "Purging Django database and static file volumes..."
  ;;
  "--purge-data")
    COMPOSE_ARGS=""
    PURGE_VOLUMES="${COMPOSE_PROJECT_NAME}_astrodash-data"
    echo "Purging astro data volume..."
  ;;
  *)
    echo "ERROR: Invalid purge option."
    exit 1
    ;;
esac

COMPOSE_CONFIG=" --profile $PROFILE"
COMPOSE_CONFIG="${COMPOSE_CONFIG} --project-name ${COMPOSE_PROJECT_NAME}"
if [[ $PROFILE == "docs" ]]; then
  COMPOSE_CONFIG="${COMPOSE_CONFIG} -f docker/${OVERLAY_FILE}"
else
  # Base first, overlay second -- the same order and the same pair
  # `astrodashctl` uses, so both address one stack rather than two.
  COMPOSE_CONFIG="${COMPOSE_CONFIG} -f docker/docker-compose.yml"
  COMPOSE_CONFIG="${COMPOSE_CONFIG} -f docker/${OVERLAY_FILE}"
  COMPOSE_CONFIG="${COMPOSE_CONFIG} --env-file env/.env.default"
  COMPOSE_CONFIG="${COMPOSE_CONFIG} --env-file ${ENV_FILE}"
fi

export COMPOSE_CONFIG
export COMPOSE_ARGS
export COMPOSE_PROJECT_NAME
export PURGE_VOLUMES
export TARGET_SERVICE
