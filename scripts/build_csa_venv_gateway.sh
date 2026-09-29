#!/usr/bin/env bash
# Build the Python environment for Flink Agents on CSA — ON THE GATEWAY NODE.
#
#   scp dist/csa/wheel/flink_agents-*.whl scripts/build_csa_venv_gateway.sh \
#       <user>@<gateway>:~/ratatoskr-csa/
#   ssh <user>@<gateway>
#   cd ~/ratatoskr-csa && ./build_csa_venv_gateway.sh
#
# WHY THIS REPLACES THE DOCKER CROSS-BUILD
# ----------------------------------------
# deploy/Dockerfile.csa-build was written on the assumption that the cluster has no
# usable Python, so the bundle had to carry its own interpreter, libpython, PyFlink,
# pemja, beam, numpy and pyarrow — 626MB, conda-packed, cross-compiled on Rocky 8 for
# glibc backward compatibility, with every version pin guessed from PyPI.
#
# That assumption is FALSE on CSA. Verified on pdf-pwc (CSA 1.18.0.0 / Flink 1.20.5),
# on the gateway AND on all three workers independently:
#
#   /usr/local/lib64/python3.11/site-packages  (installed by Cloudera, not rpm-owned)
#     apache-flink 1.20.5   apache-flink-libraries 1.20.5   apache-beam 2.48.0
#     pemja 0.5.7           numpy 1.24.4                    pyarrow 11.0.0
#     pandas 2.2.3          protobuf 4.23.4                 py4j 0.10.9.7
#     find-libpython 0.5.1  cloudpickle 2.2.1
#   /usr/lib64/libpython3.11.so.1.0   present on every node
#   $FLINK_HOME/lib/flink-python-1.20.5-csa1.18.0.0.jar   already on the system classpath
#
# So the runtime PyFlink stack is PROVISIONED, matched to the parcel by Cloudera
# themselves. Three consequences, and the third is the one that matters:
#
#   1. Size: the archive carries only what is genuinely missing, not a second copy of
#      an interpreter that is already installed on every node.
#   2. No cross-compilation: built here, on the target OS, for the target arch. The
#      Rocky-8-for-glibc-2.28 reasoning and conda-pack/conda-unpack relocation both
#      become irrelevant rather than merely working.
#   3. **pemja stops being a guess.** pemja is a JNI bridge: the Java classes in
#      flink-python-*.jar and the Python pemja_core*.so MUST be the same version.
#      Cloudera PATCHES this pin — their apache-flink 1.20.5 requires pemja>=0.5.7,<0.5.8,
#      where upstream apache-flink 1.20.1 pins pemja==0.4.1. The Docker bundle resolved
#      from PyPI and therefore contained pemja 0.4.1: it installed cleanly, imported
#      cleanly, passed a clean-RHEL9 container check, and would have died inside a
#      TaskManager with an error that reads like a classloader bug. Inheriting the node's
#      own pemja makes that class of mismatch structurally impossible.
#
# `--system-site-packages` is what makes this work: the venv inherits the node's
# installed stack instead of duplicating it, and `bin/python3.11` is a symlink to
# /usr/bin/python3.11, which exists on every node — so the archive relocates into a
# YARN container without any prefix rewriting.
#
# deploy/Dockerfile.csa-build is still required, but only for Stages 1-2: it builds the
# flink-agents JARS and the wheel from source. Its Stage 3 (the conda runtime env) is no
# longer on this path. It is kept, not deleted: a cluster without the system stack, or
# the CSA-Operator-on-AKS fallback, would need it again.

set -euo pipefail

cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

log()  { printf '\n==> %s\n' "$*"; }
fail() { printf '\nERROR: %s\n' "$*" >&2; exit 1; }

VENV="${VENV:-agentvenv}"
SYS_PY="${SYS_PY:-/usr/bin/python3.11}"
ARCHIVE="${ARCHIVE:-agentvenv.tar.gz}"
# The wheel bundles all six per-minor dist jars (~200MB of the 202MB wheel). The
# TaskManager's Python worker needs the flink_agents PYTHON code; the jars reach the TM
# via pipeline.jars as YARN local resources, not through this archive. Stripping them
# takes the archive from ~230MB to ~30MB. Default OFF for the first run so the shipped
# env is identical to the one proven on the client side — flip it on once the job runs.
STRIP_JARS="${STRIP_JARS:-0}"

# --- Premise check: the node must actually have the stack -----------------------
# If this fails, this script is the wrong tool and the Docker bundle is the right one.
[ -x "$SYS_PY" ] || fail "$SYS_PY not found.
This script's entire premise is that the node ships a PyFlink-capable Python 3.11.
Available: $(ls /usr/bin/python3.* 2>/dev/null | tr '\n' ' ')
If there is genuinely no system PyFlink, fall back to the self-contained bundle:
  scripts/build_csa_bundle.sh   (deploy/Dockerfile.csa-build, ~626MB, conda-packed)"

log "System interpreter: $SYS_PY ($("$SYS_PY" --version 2>&1))"
"$SYS_PY" - <<'PY' || fail "The system interpreter cannot import PyFlink/pemja. Use the Docker bundle path instead."
import sys
from importlib.metadata import version, PackageNotFoundError
need = ("apache-flink", "pemja", "apache-beam", "numpy", "pyarrow", "cloudpickle")
missing = []
for p in need:
    try:
        print(f"    {p:16s} {version(p)}")
    except PackageNotFoundError:
        missing.append(p)
if missing:
    print("MISSING from the system interpreter: " + ", ".join(missing), file=sys.stderr)
    sys.exit(1)
PY

# --- Cross-check the system stack against the parcel ---------------------------
# Belt and braces: these SHOULD agree, since Cloudera installs both. If they ever do
# not, the JNI halves are skewed and the job would fail inside a TaskManager.
PARCEL_CANDIDATES=(
  "/opt/cloudera/parcels/FLINK/lib/flink"
  "/opt/cloudera/parcels/CSA/lib/flink"
)
if [ -z "${FLINK_HOME:-}" ]; then
  for c in "${PARCEL_CANDIDATES[@]}"; do
    [ -d "$c/lib" ] && FLINK_HOME="$c" && break
  done
fi
if [ -n "${FLINK_HOME:-}" ] && [ -d "$FLINK_HOME/lib/flink-python-source" ]; then
  W="$FLINK_HOME/lib/flink-python-source"
  for pkg in pemja apache_flink; do
    PARCEL_V="$(ls "$W" 2>/dev/null | sed -n "s/^${pkg}-\([0-9][0-9.]*\)-.*\.whl\$/\1/p" | head -1)"
    SYS_V="$("$SYS_PY" -c "from importlib.metadata import version;print(version('${pkg//_/-}'))" 2>/dev/null || true)"
    if [ -n "$PARCEL_V" ] && [ -n "$SYS_V" ] && [ "$PARCEL_V" != "$SYS_V" ]; then
      fail "$pkg skew: system python has $SYS_V, the parcel ships $PARCEL_V.
pemja is a JNI bridge and both halves must match. Something has replaced the system
install; do not submit until the two agree."
    fi
    printf '    %-14s system %s / parcel %s\n' "$pkg" "${SYS_V:-?}" "${PARCEL_V:-?}"
  done
fi

# --- The wheel ------------------------------------------------------------------
# `|| true` because pipefail + a non-matching glob makes `ls` exit 2, which would kill the
# script before the instructions below ever print.
WHEEL="$( { ls flink_agents-*.whl 2>/dev/null || true; } | head -1)"
[ -n "$WHEEL" ] || fail "No flink_agents-*.whl here.
Build it once on the Mac and copy it over:
  scripts/build_csa_bundle.sh                        # or: docker build -f deploy/Dockerfile.csa-build
  scp dist/csa/wheel/flink_agents-*.whl <gw>:$PWD/"

# --- Create the venv ------------------------------------------------------------
log "Creating $VENV (--system-site-packages, inheriting the node's PyFlink stack)"
rm -rf "$VENV"
"$SYS_PY" -m venv --system-site-packages "$VENV"

# Pin the inherited packages so that NOTHING pip does can upgrade or shadow them. Without
# this, a transitive requirement could pull a different apache-flink or pemja into the
# venv's own site-packages, which takes precedence over the system one and silently
# recreates exactly the version skew this whole approach exists to eliminate.
log "Freezing the inherited stack so pip cannot shadow it"
"$SYS_PY" - > /tmp/csa-constraint.txt <<'PY'
from importlib.metadata import version
for p in ("apache-flink", "apache-flink-libraries", "apache-beam", "pemja", "numpy",
          "pyarrow", "pandas", "protobuf", "py4j", "cloudpickle", "find-libpython"):
    try:
        print(f"{p}=={version(p)}")
    except Exception:
        pass
print("setuptools>=75.3,<82")
PY
sed 's/^/    /' /tmp/csa-constraint.txt
export PIP_CONSTRAINT=/tmp/csa-constraint.txt PIP_BUILD_CONSTRAINT=/tmp/csa-constraint.txt

# The light deps flink-agents and ratatoskr actually need on the workflow (non-LLM) path.
# The wheel goes in with --no-deps because its declared dependency set is dominated by
# LLM/vector-store integrations (chromadb -> onnxruntime/tokenizers/grpcio, mem0ai,
# openai, anthropic, dashscope, ollama, mcp, tree-sitter) that this milestone never uses
# and that would be pure transfer cost to every TaskManager on every submission.
#
# importlib_resources and packaging are here because of the same UPSTREAM PACKAGING BUG,
# not preference: flink_agents/api/execution_environment.py imports both at module scope,
# but flink-agents 0.3 declares neither. They normally arrive transitively via chromadb, so
# the omission is invisible until you install with --no-deps.
#
# `packaging` was found by this script's own self-check on the first gateway run, and is
# worth noting as evidence the check is load-bearing rather than ceremonial: the 626MB
# Docker bundle passed without listing it, because something in the full PyPI resolve
# happened to supply it. Inheriting a leaner system stack removed that accident. Any
# further ModuleNotFoundError here is the same class of bug — add the package, do not
# drop --no-deps.
log "Installing the light dependency set"
"$VENV/bin/python" -m pip install --no-cache-dir --quiet \
    'pydantic==2.11.4' \
    'pyyaml==6.0.2' \
    'docstring-parser==0.16' \
    'importlib_resources' \
    'packaging' \
    'googleapis-common-protos<1.72.1' \
    'requests>=2.32.0' \
    'kafka-python>=2.0.2' \
    'python-dotenv>=1.0.0' \
    'ruamel.yaml' \
  || fail "pip install failed. If the gateway has no outbound internet, build a wheelhouse
on the Mac and copy it over:
  pip download -d wheelhouse --platform manylinux2014_x86_64 --python-version 3.11 \\
      --only-binary=:all: pydantic==2.11.4 pyyaml==6.0.2 docstring-parser==0.16 ...
  then re-run with: PIP_FIND_LINKS=\$PWD/wheelhouse PIP_NO_INDEX=1 ./build_csa_venv_gateway.sh"

log "Installing $WHEEL (--no-deps)"
"$VENV/bin/python" -m pip install --no-cache-dir --quiet --no-deps "$WHEEL"

# --- Prove the environment before packing it ------------------------------------
# Same check the Dockerfile's Stage 3 self-check runs, and for the same reason: a package
# that flink_agents needs EAGERLY but does not declare must fail here, not in a YARN
# container where the traceback is buried in aggregated logs.
#
# Check `pemja`, never `pemja_core`. pemja_core is the JNI extension; importing it from a
# plain CPython process ALWAYS fails with `undefined symbol: JNI_GetCreatedJavaVMs`,
# because the JVM supplies that symbol. That is correct behaviour, not a broken env — and
# it means no check on this side can prove the JNI layer loads. Only a running TaskManager
# can. The difference now is that the pemja here is the node's own, matched to the parcel,
# so the untestable part is no longer also a guess.
log "Self-check"
"$VENV/bin/python" - <<'PY' || fail "The venv cannot import its own runtime. Do not submit."
import importlib, sys
CHECKS = [
    ("pemja", ()), ("pyflink", ()), ("flink_agents", ()),
    ("apache_beam", ()), ("numpy", ()), ("pyarrow", ()), ("pydantic", ()),
    ("flink_agents.api.execution_environment", ("AgentsExecutionEnvironment",)),
    ("flink_agents.api.agents.agent", ("Agent",)),
    ("flink_agents.api.decorators", ("action", "tool")),
    ("flink_agents.api.events.event", ("Event", "InputEvent", "OutputEvent")),
    ("flink_agents.api.runner_context", ("RunnerContext",)),
]
missing, broken = {}, []
for name, attrs in CHECKS:
    try:
        mod = importlib.import_module(name)
    except ModuleNotFoundError as e:
        missing.setdefault(e.name, []).append(name); continue
    except Exception as e:
        broken.append(f"{name}: {type(e).__name__}: {e}"); continue
    for a in attrs:
        if not hasattr(mod, a):
            broken.append(f"{name}: missing attribute {a!r}")
if missing or broken:
    print("\n=== SELF-CHECK FAILED ===", file=sys.stderr)
    for pkg, by in sorted(missing.items()):
        print(f"  missing package: {pkg}  (needed by: {', '.join(by)})", file=sys.stderr)
    for b in broken:
        print(f"  broken import  : {b}", file=sys.stderr)
    if missing:
        print("\nAdd to the light dep list above: " + " ".join(sorted(missing)), file=sys.stderr)
    sys.exit(1)

from importlib.metadata import version
print("    python       :", sys.version.split()[0])
for p in ("apache-flink", "apache-beam", "pemja", "flink-agents", "pydantic"):
    print(f"    {p:13s}:", version(p))
# Where each of these resolves from matters: the PyFlink half must come from the NODE
# (/usr/local), the agents half from the VENV. If pyflink resolves inside the venv, the
# constraint file failed and a PyPI copy has shadowed Cloudera's.
import pyflink, flink_agents
print("    pyflink from :", pyflink.__file__.split("/site-packages/")[0])
print("    agents  from :", flink_agents.__file__.split("/site-packages/")[0])
if "/usr/local" not in pyflink.__file__:
    print("WARNING: pyflink is NOT the node's install — pip shadowed it.", file=sys.stderr)
    sys.exit(1)
PY

# --- Pack ------------------------------------------------------------------------
if [ "$STRIP_JARS" = "1" ]; then
  SP="$("$VENV/bin/python" -c 'import sysconfig;print(sysconfig.get_paths()["purelib"])')"
  log "STRIP_JARS=1 — removing bundled dist jars from the ARCHIVE copy only"
  rm -rf /tmp/agents-lib-backup && cp -r "$SP/flink_agents/lib" /tmp/agents-lib-backup
  find "$SP/flink_agents/lib" -name '*.jar' -delete
fi

log "Packing $ARCHIVE"
# Plain tar, not venv-pack: bin/python3.11 is a symlink to /usr/bin/python3.11 (present on
# every node) and sys.prefix is derived at runtime from pyvenv.cfg next to the executable,
# so an extracted copy works wherever YARN puts it. Nothing records the build path except
# console-script shebangs, which this path never invokes — it always calls venv/bin/python
# directly. This is why there is no conda-unpack step and no prefix-rewrite to verify.
tar czf "$ARCHIVE" "$VENV"

if [ "$STRIP_JARS" = "1" ]; then
  SP="$("$VENV/bin/python" -c 'import sysconfig;print(sysconfig.get_paths()["purelib"])')"
  log "Restoring jars in the LOCAL venv (the client resolves pipeline.jars from them)"
  rm -rf "$SP/flink_agents/lib" && cp -r /tmp/agents-lib-backup "$SP/flink_agents/lib"
  rm -rf /tmp/agents-lib-backup
fi
rm -f /tmp/csa-constraint.txt

log "Done"
du -sh "$VENV" "$ARCHIVE" | sed 's/^/    /'
cat <<EOF

    client interpreter : $PWD/$VENV/bin/python
    ships to YARN as   : $ARCHIVE  (python.archives ...#venv)

Next: ./submit_agent_csa.sh   (add DRY_RUN=1 to see the command first)
EOF
