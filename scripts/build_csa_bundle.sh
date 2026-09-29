#!/usr/bin/env bash
# Build the Flink Agents bundle for a Cloudera CSA DataHub (Flink 1.20 on YARN).
#
# Produces dist/csa/:
#   agentenv.tar.gz      self-contained conda env (python 3.11, PyFlink 1.20, pemja,
#                        flink-agents wheel, and the dist jars pre-staged inside)
#   jars/                the two Flink Agents dist jars, for -Dpipeline.jars
#   agentcode.zip        ratatoskr runtime + example agents, for -pyfs
#   run_workflow_cluster_csa.py   job entry point, for -py
#   BUILD-INFO.txt       what was built, so the cluster can be cross-checked
#
# Usage:
#   scripts/build_csa_bundle.sh                       # defaults (Flink 1.20, amd64)
#   FLINK_MAJOR_MINOR=1.20 PLATFORM=linux/amd64 scripts/build_csa_bundle.sh
#   BASE_IMAGE=rockylinux/rockylinux:9 scripts/build_csa_bundle.sh
#
# Run Phase 0's probes on the gateway FIRST. Three of its answers change this build:
# the OS family (BASE_IMAGE), the architecture (PLATFORM), and the exact Flink
# version in the parcel (FLINK_MAJOR_MINOR / FLINK_PATCH_VERSION).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# Cluster nodes are x86_64; an Apple Silicon Mac must cross-build or pemja's
# compiled extension is the wrong architecture and will not load on the TaskManager.
PLATFORM="${PLATFORM:-linux/amd64}"
BASE_IMAGE="${BASE_IMAGE:-rockylinux/rockylinux:8}"
FLINK_AGENTS_VERSION="${FLINK_AGENTS_VERSION:-release-0.3}"
FLINK_MAJOR_MINOR="${FLINK_MAJOR_MINOR:-1.20}"
FLINK_PATCH_VERSION="${FLINK_PATCH_VERSION:-1.20.1}"
PYTHON_VERSION="${PYTHON_VERSION:-3.11}"
# Not from dnf: RHEL8's Maven 3.5.4 is too old for flink-agents' build plugins.
MAVEN_VERSION="${MAVEN_VERSION:-3.9.9}"
IMAGE_TAG="${IMAGE_TAG:-ratatoskr-csa-build:${FLINK_MAJOR_MINOR}}"
OUT_DIR="$REPO_ROOT/dist/csa"

log() { printf '\n==> %s\n' "$*"; }

mkdir -p "$OUT_DIR"

# The Maven + conda build takes tens of minutes; the code zip takes a second. When only
# agent code changed, SKIP_DOCKER=1 refreshes agentcode.zip against an existing bundle.
if [ "${SKIP_DOCKER:-0}" = "1" ]; then
  log "SKIP_DOCKER=1 — reassembling agentcode.zip only, leaving the env/jars in place"
else

log "Building $IMAGE_TAG for $PLATFORM (base: $BASE_IMAGE, Flink $FLINK_PATCH_VERSION, Python $PYTHON_VERSION)"
docker build \
  --platform "$PLATFORM" \
  -f deploy/Dockerfile.csa-build \
  --build-arg "BASE_IMAGE=$BASE_IMAGE" \
  --build-arg "FLINK_AGENTS_VERSION=$FLINK_AGENTS_VERSION" \
  --build-arg "FLINK_MAJOR_MINOR=$FLINK_MAJOR_MINOR" \
  --build-arg "FLINK_PATCH_VERSION=$FLINK_PATCH_VERSION" \
  --build-arg "PYTHON_VERSION=$PYTHON_VERSION" \
  --build-arg "MAVEN_VERSION=$MAVEN_VERSION" \
  -t "$IMAGE_TAG" \
  .

log "Extracting artifacts to $OUT_DIR"
# Copy out of a stopped container rather than `docker run`, so nothing executes.
CID="$(docker create --platform "$PLATFORM" "$IMAGE_TAG" /bin/true)"
trap 'docker rm -f "$CID" >/dev/null 2>&1 || true' EXIT
docker cp "$CID:/out/agentenv.tar.gz"        "$OUT_DIR/"
docker cp "$CID:/out/jars"                   "$OUT_DIR/"
docker cp "$CID:/out/BUILD-INFO.txt"         "$OUT_DIR/"
docker cp "$CID:/out/site-packages-path.txt" "$OUT_DIR/"

fi   # end SKIP_DOCKER

log "Assembling agentcode.zip"
# Only the modules a cluster job actually imports. Deliberately excludes the
# Docker-orchestration surface (ratatoskr/agents/submit.py, designer/, pipelines/,
# commands/) — none of it runs on a YARN node, and honeypot/ is bytecode-only stubs
# that cannot be shipped at all.
STAGE="$(mktemp -d)"
trap 'docker rm -f "${CID:-}" >/dev/null 2>&1 || true; rm -rf "$STAGE"' EXIT
# zip appends to an existing archive; start clean so removed modules really disappear.
rm -f "$OUT_DIR/agentcode.zip"

mkdir -p "$STAGE/ratatoskr/runtime" "$STAGE/ratatoskr/agents" "$STAGE/examples/agents"

for f in __init__.py constants.py paths.py flink_rest.py kafka_sources.py; do
  [ -f "ratatoskr/$f" ] && cp "ratatoskr/$f" "$STAGE/ratatoskr/$f"
done
cp ratatoskr/runtime/*.py "$STAGE/ratatoskr/runtime/"
for f in __init__.py published_copy.py; do
  [ -f "ratatoskr/agents/$f" ] && cp "ratatoskr/agents/$f" "$STAGE/ratatoskr/agents/$f"
done

[ -f examples/__init__.py ] && cp examples/__init__.py "$STAGE/examples/__init__.py" \
  || touch "$STAGE/examples/__init__.py"
cp examples/agents/*.py "$STAGE/examples/agents/" 2>/dev/null || true
# Published-shim agents load their implementation from .ratatoskr/, which is
# gitignored local Designer state and absent on the cluster; importing them would
# raise at module import time. Drop them.
rm -rf "$STAGE/examples/agents/published_shims"

# The importable packages are the zip's own top-level entries, with no wrapper directory:
# Flink expands a -pyfs zip into the very directory it then puts on the worker's PYTHONPATH
# (AbstractPythonEnvironmentManager.constructFilesDirectory), so anything nested one level
# deeper is invisible.
#
# This zip is an INPUT to submit_agent_csa.sh, not the file handed to -pyfs. That script
# unpacks it and adds flink_agents plus its dependencies from the gateway venv, which cannot
# happen here because the venv is built on the cluster node, not on this machine.
( cd "$STAGE" && zip -qr "$OUT_DIR/agentcode.zip" ratatoskr examples \
    -x '*__pycache__*' -x '*.pyc' )

# The entry script is shipped twice on purpose: -py needs a real file on disk, and
# the copy inside the zip keeps the module importable by the same name on workers.
cp examples/agents/run_workflow_cluster_csa.py "$OUT_DIR/"
# The submit script runs from the bundle root (it cd's to its own directory, which is
# where agentenv.tar.gz and jars/ must be), so it has to travel with the bundle.
cp scripts/submit_agent_csa.sh "$OUT_DIR/"
chmod +x "$OUT_DIR/submit_agent_csa.sh"

log "Bundle contents"
( cd "$OUT_DIR" && ls -la && echo && { [ -f BUILD-INFO.txt ] && cat BUILD-INFO.txt \
    || echo "(no BUILD-INFO.txt — run without SKIP_DOCKER=1 to build the environment)"; } )

cat <<EOF

==> Next: ship to the gateway and submit

    scp -r dist/csa/ <user>@<gateway>:~/ratatoskr-csa/
    ssh <user>@<gateway>
    kinit <user>
    cd ~/ratatoskr-csa && DRY_RUN=1 ./submit_agent_csa.sh   # inspect the command first
    cd ~/ratatoskr-csa && ./submit_agent_csa.sh             # then submit

The submit script re-checks, on the gateway, the things that can only be checked there:
that the parcel ships PyFlink at all, that the bundle's Flink minor matches the
cluster's, and that the unpacked environment can import pemja/pyflink/flink_agents.

EOF
