# Developer Getting Started

## Quick Start

```bash
# Start the full development environment
run/astrodashctl full_dev up

# View logs
run/astrodashctl full_dev logs

# Stop the environment
run/astrodashctl full_dev down
```

The application will be available at `http://localhost:4000/astrodash/`.

### Running Tests

The test script execs into an **already-running** stack rather than starting
one, so bring the stack up first:

```bash
run/astrodashctl slim_dev up
run/astrodash.test.sh slim_dev
```

Pass the same profile to both. If the stack is not up, the script says so and
names the command that starts it.

To run one module while iterating, exec directly:

```bash
docker compose $(source run/get_compose_args.sh slim_dev >/dev/null; echo $COMPOSE_CONFIG) \
  exec app python manage.py test astrodash.tests.test_model_access -v 2
```

### Development Profiles

| Profile | Command | Description |
|---------|---------|-------------|
| `full_dev` | `run/astrodashctl full_dev up` | Web app, database, Redis cache, and nginx |
| `slim_dev` | `run/astrodashctl slim_dev up` | Web app and database only (no Redis cache) |

### Running a gated model locally

A model definition can declare that reaching it requires a shared access code
(`requires_credential=True`). Such a model is unlisted: it appears on no
selection page, the REST API refuses it, and the only way in is a model-scoped
entry link. `docs/admin/gated-model-access.md` covers this for a deployment;
this section is the local equivalent, for developing or demonstrating one.

**1. Put the weights on the local data volume.** A gated model's weights are
generally not in the repository, the image, or the data bucket, so copy them in
by hand. The volume outlives `down`, so this is a one-time step per machine:

```bash
docker exec astrodash-dev-app-1 mkdir -p /mnt/astrodash-data/pre_trained_models/<model>
docker cp <local-weights>.pt astrodash-dev-app-1:/mnt/astrodash-data/pre_trained_models/<model>/
```

`purge-data` and `purge-all` delete that volume and the weights with it.

**2. Configure the gate.** A stack whose registry contains a gated model
**refuses to start** unless all three are set, so this is not optional. Put them
in `env/.env.dev`, which is git-ignored -- never in `env/.env.default` or
`env/.env.ci`, which are tracked and public:

```bash
ASTRODASH_MODEL_GATE_CREDENTIAL=<a throwaway local code>
ASTRODASH_MODEL_GATE_LINK_TTL_SECONDS=3600
ASTRODASH_MODEL_GATE_LINK_BASE_URL=http://localhost:4000
SECRET_KEY=<any non-placeholder value>
```

`SECRET_KEY` signs entry links, so it must not be left at the committed
`django-insecure-` default -- the gate treats that as unconfigured and fails
closed. Restart the stack after editing the file; compose reads env files at
`up`, not per request.

**3. Mint a link and find the code.** No route mints a link, so it comes from a
shell in the running container:

```bash
# Prints one URL and nothing else. Never prints the access code.
docker exec astrodash-dev-app-1 python manage.py mint_model_link <model-id>

# A longer window than the configured default, in seconds
docker exec astrodash-dev-app-1 python manage.py mint_model_link <model-id> --ttl-seconds 7200

# The code you will be prompted for
grep ASTRODASH_MODEL_GATE_CREDENTIAL env/.env.dev
```

Open the link, enter the code, and you land in a session scoped to that one
model: classification only, no batch flow and no model picker, with an explicit
**End session** control.

The deadline is stamped into the link when it is minted, so changing the TTL
afterwards does not move an existing link's expiry in either direction. Mint at
the point of use.

**Demonstrating to someone else.** nginx binds to `127.0.0.1:4000` and links are
minted against `localhost`, so the stack is not reachable from another machine.
Either share your screen, or have them tunnel, after which the minted links work
verbatim in their browser:

```bash
ssh -L 4000:localhost:4000 <your-host>
```

### Version string in the footer

On the `full_dev` and `slim_dev` profiles, `astrodashctl` runs `git describe --tags --always` on the host at up-time and exports the result as `APP_VERSION`. The Django app reads it and shows it in the page footer, in the `/healthz` payload, and in the startup log line emitted by `AstroDashConfig.ready()`. So the footer names the commit the stack was launched from — useful when running multiple worktrees or branches side by side.

The value is a snapshot at up-time, not live per request. A commit made after `astrodashctl full_dev up` doesn't refresh the footer until the next `up` (`docker compose restart app` alone does not re-interpolate compose vars). `--dirty` is intentionally omitted: the dev overlay bind-mounts `app/` into the container, so running code diverges from the snapshotted working tree the moment you edit after `up`, and a stale `-dirty` (or non-dirty) suffix would report false state.

To display a specific string instead — for example, when testing what a release tag will render:

```bash
APP_VERSION=v1.2.3 run/astrodashctl full_dev up
```

Non-dev profiles (`full_prod`, `slim_prod`, `ci`, `docs`) and running `docker compose up` directly (bypassing `astrodashctl`) fall back to the literal `local`. The operator-side counterpart — how the Helm chart sets `APP_VERSION` on the deployed pod from the image tag — is documented in `docs/operator-runbook.md`.
