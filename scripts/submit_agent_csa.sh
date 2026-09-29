#!/usr/bin/env bash
# Submit the Flink Agents workflow_counter job to YARN on a Cloudera CSA DataHub.
#
# Runs ON THE GATEWAY NODE:
#
#   scp -r dist/csa/ <user>@<gateway>:~/ratatoskr-csa/
#   ssh <user>@<gateway>
#   cd ~/ratatoskr-csa
#   ./build_csa_venv_gateway.sh     # builds agentvenv/ + agentvenv.tar.gz here
#   kinit <workload-user>
#   ./submit_agent_csa.sh
#
# The environment is built HERE, on the gateway, by build_csa_venv_gateway.sh — not
# cross-compiled on a Mac. CSA nodes ship a complete matched PyFlink stack in
# /usr/local/lib64/python3.11/site-packages (apache-flink 1.20.5, pemja 0.5.7, beam
# 2.48.0, numpy, pyarrow, pandas) plus /usr/lib64/libpython3.11.so, on every node, so a
# --system-site-packages venv inherits all of it and adds only the flink-agents wheel and
# a few light deps. That is why there is no conda-unpack step here: bin/python3.11 is a
# symlink to /usr/bin/python3.11, which exists on every node, and sys.prefix is derived at
# runtime from pyvenv.cfg, so a plain tar relocates into a YARN container as-is.
#
# Environment overrides:
#   FLINK_HOME      Flink parcel root (auto-detected; see PARCEL_CANDIDATES)
#   VENV            client-side venv dir (default agentvenv)
#   ARCHIVE         vestigial (default agentvenv.tar.gz); nothing is shipped via HDFS any
#                   more — the worker libraries ride inside the -pyfs zip instead
#   SYS_PY          worker interpreter (default /usr/bin/python3.11)
#   JOB_NAME        YARN application name
#   ENTRY           entry script (default run_workflow_cluster_csa.py)
#   KEYTAB          + PRINCIPAL: authenticate from a keytab instead of a ticket cache
#   DRY_RUN=1       print the flink command and exit without submitting
#
# NOTE: this is a deliberately thin wrapper around `flink run`. It is NOT a port of
# ratatoskr/agents/submit.py:submit_agent_cluster(), which is Docker-shaped
# (docker cp of ~40 files into two containers, docker exec, Designer DB sync) and
# has already been forked once into honeypot/src/cluster/. Generalising that behind
# a backend interface is the right refactor, but only once this command is known to
# work — otherwise it is a design against guesses.

set -euo pipefail

cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

log()  { printf '\n==> %s\n' "$*"; }
fail() { printf '\nERROR: %s\n' "$*" >&2; exit 1; }

VENV="${VENV:-agentvenv}"
ARCHIVE="${ARCHIVE:-agentvenv.tar.gz}"
# The node's system interpreter — the one the TaskManagers run, and the one that owns the
# stdlib plus Cloudera's PyFlink/pemja. Must be the same interpreter the venv was built
# against (build_csa_venv_gateway.sh defaults to the same path).
SYS_PY="${SYS_PY:-/usr/bin/python3.11}"

# --- Locate the Flink parcel ---------------------------------------------------
PARCEL_CANDIDATES=(
  "/opt/cloudera/parcels/FLINK/lib/flink"
  "/opt/cloudera/parcels/CSA/lib/flink"
  "/opt/cloudera/parcels/FLINK-1.20.1/lib/flink"
)
if [ -z "${FLINK_HOME:-}" ]; then
  for c in "${PARCEL_CANDIDATES[@]}"; do
    [ -d "$c/lib" ] && FLINK_HOME="$c" && break
  done
fi
[ -n "${FLINK_HOME:-}" ] && [ -d "$FLINK_HOME/lib" ] \
  || fail "Could not find the Flink parcel. Set FLINK_HOME explicitly.
Tried: ${PARCEL_CANDIDATES[*]}
Find it with: ls -d /opt/cloudera/parcels/*/lib/flink"
export FLINK_HOME

# --- FLINK_CONF_DIR is MANDATORY on a parcel install -----------------------------
# The parcel has NO conf/ directory. bin/config.sh line 208 does
#     if [ -z "$FLINK_CONF_DIR" ]; then FLINK_CONF_DIR=$FLINK_HOME/conf; fi
# so with it unset, `flink run` silently runs with NO CONFIGURATION AT ALL. Cloudera
# Manager writes the real config to /etc/flink/conf (a symlink via alternatives to
# conf.cloudera.flink) holding flink-conf.yaml, log4j-cli.properties and log4j.properties.
#
# This is not cosmetic. Observed on the first real submission, which failed with:
#     sed: can't read .../lib/flink/conf/config.yaml: No such file or directory
#     main ERROR Reconfiguration failed: No configuration found for '30946e09'
# Three distinct things break, and the first is the dangerous one:
#
#   1. env.java.opts.all is lost, which is where CSA puts
#      --add-opens=java.base/java.net=ALL-UNNAMED. PyFlink's add_jars ->
#      add_jars_to_context_class_loader reflects on URLClassLoader.addURL, so without
#      that opens it throws InaccessibleObjectException on Java 17. Flink Agents'
#      attach_flink_agents_jars wraps add_jars in `try/except Exception: pass`, so the
#      failure is SWALLOWED, ZERO agents jars get attached, and the job dies later with a
#      ClassNotFoundException that points nowhere near the real cause.
#   2. No log4j-cli.properties -> the "Reconfiguration failed" noise above.
#   3. No YARN, HA, or security defaults from flink-conf.yaml.
CONF_CANDIDATES=("/etc/flink/conf" "/etc/flink/conf.cloudera.flink")
if [ -z "${FLINK_CONF_DIR:-}" ]; then
  for c in "${CONF_CANDIDATES[@]}"; do
    [ -f "$c/flink-conf.yaml" ] || [ -f "$c/config.yaml" ] || continue
    FLINK_CONF_DIR="$c"; break
  done
fi
[ -n "${FLINK_CONF_DIR:-}" ] \
  || fail "No Flink config dir found. Set FLINK_CONF_DIR explicitly.
Tried: ${CONF_CANDIDATES[*]}
The parcel's own $FLINK_HOME/conf does not exist on a CM-managed install; Cloudera Manager
writes the client config to /etc/flink/conf. Find it with:
  ls -l /etc/flink/conf/ ; alternatives --display flink-conf"
export FLINK_CONF_DIR

# --- HADOOP_CLASSPATH is MANDATORY for YARN submission ---------------------------
# Flink does not bundle Hadoop. bin/config.sh appends $HADOOP_CLASSPATH to the CLI
# classpath, and with it unset the CLI cannot even construct itself — it dies in
# CliFrontend.<init> while loading filesystem factories:
#     java.lang.NoClassDefFoundError: org/apache/hadoop/conf/Configuration
#     ServiceConfigurationError: ...S3AFileSystemFactory Unable to get public no-arg ctor
# The S3/GCS factory errors are a red herring: they are consequences of Hadoop being
# absent, not of anything to do with object storage. This is required for YARN deployment
# regardless of where state lives.
if [ -z "${HADOOP_CLASSPATH:-}" ]; then
  command -v hadoop >/dev/null 2>&1 \
    || fail "HADOOP_CLASSPATH is unset and no 'hadoop' on PATH to derive it from.
Flink ships without Hadoop and cannot submit to YARN without it. Set it explicitly:
  export HADOOP_CLASSPATH=\$(hadoop classpath)"
  HADOOP_CLASSPATH="$(hadoop classpath)"
fi
export HADOOP_CLASSPATH
# CM's own wrapper defaults this, but set it so the value is explicit and logged.
export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-/etc/hadoop/conf}"

# --- Use Cloudera's wrapper, not lib/flink/bin/flink -----------------------------
# THE IMPORTANT LESSON OF THIS SCRIPT. The parcel has TWO flink executables:
#
#   /opt/cloudera/parcels/FLINK/bin/flink       <- the wrapper (also /usr/bin/flink)
#   /opt/cloudera/parcels/FLINK/lib/flink/bin/flink   <- vanilla Flink, what the wrapper execs
#
# The wrapper sources bin/flink-exec-env.sh first, which sets JAVA_HOME (via
# bigtop-detect-javahome), HADOOP_HOME, HADOOP_CONF_DIR, HADOOP_CLASSPATH=$(hadoop
# classpath), HBASE_CONF_DIR, FLINK_CONF_DIR (/etc/flink/conf when present), FLINK_LOG_DIR,
# and — the one that is genuinely hard to guess —
#     ATLAS_CLASSPATH=$(find lib/flink/opt/cloudera/atlas/ -maxdepth 1 -name "*.jar")
#     FLINK_USER_CLASSPATH=$ATLAS_CLASSPATH
#
# Calling the inner binary directly cost three failed submissions, each looking unrelated:
#   1. sed: can't read .../lib/flink/conf/config.yaml   (no FLINK_CONF_DIR)
#   2. NoClassDefFoundError: org/apache/hadoop/conf/Configuration   (no HADOOP_CLASSPATH)
#   3. Could not load JobListener : org.apache.atlas.flink.hook.FlinkAtlasHook
#      (no FLINK_USER_CLASSPATH — CSA sets execution.job-listeners to the Atlas hook in
#      flink-conf.yaml with atlas.collection.enabled: true, so EVERY job loads it)
#
# Every one of those was self-inflicted. Prefer the wrapper. It honours anything already
# exported (${VAR:-default}), so the values set above still win where they are set.
PARCEL_ROOT="$(cd "$FLINK_HOME/../.." 2>/dev/null && pwd || true)"
for c in /usr/bin/flink "$PARCEL_ROOT/bin/flink"; do
  [ -x "$c" ] && [ "$c" != "$FLINK_HOME/bin/flink" ] || continue
  grep -q flink-exec-env "$c" 2>/dev/null || continue
  FLINK_BIN="$c"; break
done
if [ -z "${FLINK_BIN:-}" ]; then
  FLINK_BIN="$FLINK_HOME/bin/flink"
  # No wrapper: reproduce the part of its job that nothing else will.
  if [ -z "${FLINK_USER_CLASSPATH:-}" ] && [ -d "$FLINK_HOME/opt/cloudera/atlas" ]; then
    FLINK_USER_CLASSPATH="$(find "$FLINK_HOME/opt/cloudera/atlas/" -maxdepth 1 -name '*.jar' | paste -sd: -)"
    export FLINK_USER_CLASSPATH
  fi
fi

DIST_JAR="$(ls "$FLINK_HOME"/lib/flink-dist*.jar 2>/dev/null | head -1)" \
  || fail "No flink-dist jar under $FLINK_HOME/lib"
log "Flink parcel: $FLINK_HOME"
echo "    dist jar: $(basename "$DIST_JAR")"
echo "    conf dir: $FLINK_CONF_DIR"
echo "    hadoop  : $HADOOP_CONF_DIR + $(echo "$HADOOP_CLASSPATH" | tr ':' '\n' | wc -l | tr -d ' ') classpath entries"
echo "    flink   : $FLINK_BIN$([ "$FLINK_BIN" = "$FLINK_HOME/bin/flink" ] && echo '  (raw binary — no CM wrapper found)' || echo '  (CM wrapper)')"
# Confirm the opens that add_jars depends on is actually present, rather than trusting it.
if ! grep -q 'add-opens=java.base/java.net' "$FLINK_CONF_DIR"/flink-conf.yaml \
                                            "$FLINK_CONF_DIR"/config.yaml 2>/dev/null; then
  echo "    WARNING: --add-opens=java.base/java.net=ALL-UNNAMED not found in the config." >&2
  echo "    On Java 17 PyFlink's add_jars can then fail SILENTLY and attach no jars." >&2
fi

# --- The go/no-go check -------------------------------------------------------
# Look in lib/ AND opt/. Vanilla Flink ships flink-python in opt/ and requires you to
# copy it into lib/; Cloudera ships it ALREADY IN lib/, i.e. on the system classpath.
# This check previously looked only in opt/ and would have aborted a perfectly valid
# submission against a CSA parcel that does support PyFlink. Verified on
# pdf-pwc (CSA 1.18.0.0 / Flink 1.20.5): lib/flink-python-1.20.5-csa1.18.0.0.jar.
# The `|| true` is load-bearing, not defensive noise. `set -o pipefail` is on, and `ls`
# given two globs where only one matches still exits 2 — so without it the assignment
# fails, `set -e` kills the script, and the careful error message below never prints.
# That is exactly what happened on the first run against this parcel: the jar WAS found
# (in lib/) and the script still died silently. Every `ls <glob> | ...` here where a
# non-match is a legitimate outcome needs the same guard.
PYFLINK_JAR="$( { ls "$FLINK_HOME"/lib/flink-python*.jar "$FLINK_HOME"/opt/flink-python*.jar \
                    2>/dev/null || true; } | head -1)"
if [ -z "$PYFLINK_JAR" ]; then
  fail "No flink-python jar in $FLINK_HOME/lib/ or $FLINK_HOME/opt/ — no PyFlink support.
Contents of $FLINK_HOME/opt/:
$(ls "$FLINK_HOME/opt/" 2>/dev/null | sed 's/^/    /')
python-related in $FLINK_HOME/lib/:
$(ls "$FLINK_HOME/lib/" 2>/dev/null | grep -i python | sed 's/^/    /')
Fall back to the CSA Operator on AKS path (see the runbook)."
fi
echo "    pyflink jar: $(basename "$PYFLINK_JAR")"

PARCEL_WHEELS="$FLINK_HOME/lib/flink-python-source"

# --- The environment -----------------------------------------------------------
[ -x "$VENV/bin/python" ] || fail "$VENV/bin/python not found.
Build the environment on this node first:
  ./build_csa_venv_gateway.sh
That is deliberately a separate step: it needs the flink_agents wheel and (unless you
supply a wheelhouse) outbound internet for a handful of light dependencies, neither of
which should be on the critical path of a resubmit."
# NOTE: $ARCHIVE is no longer required. It used to ship to the TaskManagers via -pyarch;
# it now ships nowhere (see "Why the archive is gone" below) and the venv is used only
# client-side, straight from the directory. Left unchecked rather than removed so an older
# bundle that still carries the tarball is not rejected for having it.

# Import the same API surface the build's self-check asserts, not just the top-level
# packages. The flink-agents wheel is installed with --no-deps, so a dependency it needs
# eagerly but does not declare (importlib_resources and packaging are both real examples)
# would leave top-level imports working while the job fails inside a YARN container.
#
# Check `pemja`, never `pemja_core`. pemja_core is the JNI extension, and importing it
# from a plain CPython process ALWAYS fails with
#     ImportError: pemja_core...so: undefined symbol: JNI_GetCreatedJavaVMs
# because that symbol is supplied by the JVM that embeds the interpreter. That is correct
# behaviour, not a broken env. Consequently NO check on this side can prove the JNI layer
# loads — only a running TaskManager can. What changed is that the pemja being loaded is
# now the NODE'S OWN, matched to the parcel by Cloudera, so the untestable part is no
# longer also a guess.
# `if !` rather than a trailing `$?` test, so this does not depend on the `set -e` above.
if ! "$VENV/bin/python" - <<'PY'
import sys
for m in ("pyflink", "pemja", "flink_agents",
          "flink_agents.api.execution_environment",
          "flink_agents.api.agents.agent",
          "flink_agents.api.decorators",
          "flink_agents.api.events.event",
          "flink_agents.api.runner_context"):
    __import__(m)
import pyflink, flink_agents
print("    env imports ok:", sys.version.split()[0])
print("    pyflink from  :", pyflink.__file__.split("/site-packages/")[0])
print("    agents  from  :", flink_agents.__file__.split("/site-packages/")[0])
PY
then
  fail "The venv cannot import its own runtime. Rebuild it: ./build_csa_venv_gateway.sh
If the traceback names a missing module, add it to that script's light dependency list."
fi

# --- pemja must match the parcel exactly --------------------------------------
# With a --system-site-packages venv these should ALWAYS agree, since both halves come
# from Cloudera. The check stays because the failure it catches is silent and expensive:
# if anything ever installs a second pemja or apache-flink into the venv's own
# site-packages, that copy SHADOWS the system one (venv site-packages precedes
# /usr/local on sys.path) and the JNI halves skew. pemja is a JNI bridge — the Java
# classes in flink-python-*.jar and the Python pemja_core*.so must be the same version,
# or the job dies inside a TaskManager with a pemja ClassCast/UnsatisfiedLinkError that
# reads like a classloader problem and sends you hunting FLINK-39226 instead of a
# version skew. Cloudera PATCHES this pin: CSA's apache-flink 1.20.5 requires
# pemja>=0.5.7,<0.5.8, where UPSTREAM apache-flink 1.20.1 pins pemja==0.4.1.
if [ -d "$PARCEL_WHEELS" ]; then
  for pkg in pemja apache_flink; do
    dist="${pkg//_/-}"
    PARCEL_V="$(ls "$PARCEL_WHEELS" 2>/dev/null \
      | sed -n "s/^${pkg}-\([0-9][0-9.]*\)-.*\.whl\$/\1/p" | head -1)"
    VENV_V="$("$VENV/bin/python" -c \
      "from importlib.metadata import version; print(version('$dist'))" 2>/dev/null || true)"
    if [ -n "$PARCEL_V" ] && [ -n "$VENV_V" ] && [ "$PARCEL_V" != "$VENV_V" ]; then
      fail "$dist mismatch: venv resolves $VENV_V, this parcel ships $PARCEL_V.
Something has installed a copy into the venv that shadows the node's install. Check:
  $VENV/bin/python -c 'import ${pkg%%_*}; print(${pkg%%_*}.__file__)'
It must resolve under /usr/local, NOT under $PWD/$VENV. Rebuild with
./build_csa_venv_gateway.sh, which pins the inherited versions in a pip constraint file
precisely to stop this."
    fi
    printf '    %-14s venv %s / parcel %s\n' "$dist" "${VENV_V:-?}" "${PARCEL_V:-?}"
  done
fi

# --- Kerberos -----------------------------------------------------------------
if [ -n "${KEYTAB:-}" ]; then
  [ -n "${PRINCIPAL:-}" ] || fail "KEYTAB set but PRINCIPAL is not."
  [ -f "$KEYTAB" ] || fail "Keytab not found: $KEYTAB"
  # -yD, not -D: FlinkYarnSessionCli is the active CustomCommandLine on CSA and never reads
  # -D, so a keytab passed that way is silently ignored and the job quietly falls back to the
  # submitting user's ticket cache — which expires, killing a long-running job hours later for
  # no visible reason. See the -yD banner at the FLINK_CMD assembly below. This also matches
  # cdf_flink_tutorials/flink-tutorials/flink-secure-tutorial, which uses -yD throughout.
  KERBEROS_OPTS=(
    -yD security.kerberos.login.keytab="$KEYTAB"
    -yD security.kerberos.login.principal="$PRINCIPAL"
  )
  log "Kerberos: keytab $PRINCIPAL"
else
  KERBEROS_OPTS=()
  # DRY_RUN must not require a ticket. Everything above this point (parcel, PyFlink jar,
  # venv imports, pemja/parcel agreement, jar-vs-cluster version match) is checkable
  # without credentials, and those are the checks worth running early — needing a
  # Kerberos password just to see the assembled command would make a dry run useless
  # exactly when it is most useful.
  if ! klist -s 2>/dev/null; then
    [ "${DRY_RUN:-0}" = "1" ] \
      || fail "No Kerberos ticket. Run: kinit <workload-user>
NOTE: the OS login account (cloudbreak) is NOT a Kerberos principal — it has no ticket
and cannot reach HDFS or YARN. Use your CDP WORKLOAD username, whose password is set in
the CDP console under Management Console > User Management > your user > Set Workload
Password.
(Or pass KEYTAB=... PRINCIPAL=... — required for long-running jobs, since a ticket
cache expires and the job then loses access to HDFS.)
Re-run with DRY_RUN=1 to validate everything else without a ticket."
    log "Kerberos: NO TICKET — dry run only"
  else
    log "Kerberos: ticket cache ($(klist 2>/dev/null | sed -n 's/^Default principal: //p'))"
  fi
fi

# --- Assemble the -pyfs payload ------------------------------------------------
# WHY THE ARCHIVE IS GONE, AND WHY THE ZIP CARRIES THE LIBRARIES INSTEAD.
#
# Measured on the TaskManager (app ..._0004), the python worker's PYTHONPATH had exactly
# ONE entry:
#
#     PYTHONPATH of python worker: .../python-dist-<uuid>/python-files/agentcode.zip/agentcode
#
# Two facts follow, and together they retire the whole python.archives approach:
#
#  1. The extracted archives directory contributes NOTHING to PYTHONPATH. `python-archives`
#     is where -pyarch lands, and Flink never puts it on the path. The only thing that reads
#     from there is the interpreter named by -pyexec. Since -pyexec must be the node's
#     interpreter (PYTHONHOME/`No module named 'encodings'` — see below), the archive had no
#     remaining job at all.
#  2. `python.pythonpath` cannot rescue it: like the other -D python options it is silently
#     dropped by the CLI. Verified the same way as before — the TM's "Loading configuration
#     property: python.*" lines list archives, executable, client.executable, files and the
#     two internal key-maps, and NO pythonpath. There is a dedicated `-pypath` flag for it,
#     but there is no longer any reason to use it.
#
# The zip, by contrast, is a proven path: -pyfs is what puts anything on the worker's
# PYTHONPATH. So flink_agents and its pure-Python dependencies travel INSIDE the zip.
#
# THE ZIP'S ENTRIES ARE THE TOP-LEVEL PACKAGES. No wrapper directory. From Flink 1.20's
# AbstractPythonEnvironmentManager.constructFilesDirectory():
#
#     targetDirectory = <files dir>/<cache name>/<origin name minus ".zip">
#     FileUtils.expandDirectory(zip, targetDirectory);   // contents land INSIDE it
#     pythonPath = targetDirectory;
#
# So the directory placed on PYTHONPATH is the one the zip is expanded into. `ratatoskr/`
# and `examples/` sitting at the zip root is therefore already right, and wrapping them in
# an `agentcode/` directory is wrong — it buries them one level below PYTHONPATH. (Learned
# the hard way: that wrapper was added on the theory that the derived path
# `agentcode.zip/agentcode` did not exist. It always existed. The ..._0004 failure was the
# simpler thing — flink_agents was not in the zip at all, only in the archive, and the
# archive is not on PYTHONPATH.)
#
# Flink EXPANDS the zip on the worker rather than using zipimport, which is what makes it
# legal to ship compiled extensions (pydantic_core, _yaml) inside it.
#
# The embedded interpreter does read this: flink-agents' own PythonEnvironmentManager
# (runtime/.../env/PythonEnvironmentManager.java) extends Flink's AbstractPythonEnvironmentManager
# and does `.addPythonPaths(env.getOrDefault("PYTHONPATH", ""))` when building the pemja
# PythonInterpreterConfig. -pyfs is the supported route to flink_agents on a worker.
#
# Why this never bites the local Docker path: there, flink_agents is installed into
# /opt/flink/pythonpath/agent-site-packages, which is on the TaskManager container's own
# PYTHONPATH, so the dependency-shipping route is never exercised.
PYFS_DIR="$PWD/pyfs"
PYFS_ZIP="$PYFS_DIR/agentcode.zip"
# The agent code is staged TWICE, and the split is load-bearing rather than tidiness:
#
#   client/    ratatoskr + examples ONLY. Goes on the client's PYTHONPATH.
#   payload/   the same code PLUS flink_agents and friends. Zipped for the workers.
#
# The client must NOT see the copy of flink_agents assembled below, because the worker copy
# has its lib/*.jar stripped. On the client that directory would shadow the venv's complete
# flink_agents (PYTHONPATH precedes site-packages), flink_agents_jar_uris() would return an
# empty list, and the jars would silently never be attached — the exact failure mode the
# preflight in run_workflow_cluster_csa.py exists to catch.
CODE_DIR="$PYFS_DIR/client"
PAYLOAD_DIR="$PYFS_DIR/payload"
log "Assembling $PYFS_ZIP (agent code + flink_agents runtime)"
[ -f agentcode.zip ] || fail "agentcode.zip not found. Did you scp the whole dist/csa/ directory?"
rm -rf "$PYFS_DIR"
mkdir -p "$CODE_DIR" "$PAYLOAD_DIR"
unzip -qo agentcode.zip -d "$CODE_DIR"
cp -r "$CODE_DIR/." "$PAYLOAD_DIR/"

VENV_SP="$("$VENV/bin/python" -c 'import sysconfig;print(sysconfig.get_paths()["purelib"])')"
# Only what the agent runtime imports. Deliberately excludes pip/setuptools/pkg_resources
# and _distutils_hack: 23MB of build tooling that no YARN container needs, and the node's
# system interpreter already provides setuptools. Everything else here is either
# flink_agents itself or something it imports eagerly.
for dep in flink_agents pydantic pydantic_core annotated_types typing_inspection \
           docstring_parser importlib_resources packaging yaml _yaml dotenv kafka google; do
  [ -e "$VENV_SP/$dep" ] && cp -r "$VENV_SP/$dep" "$PAYLOAD_DIR/"
done
# _yaml may also be a bare extension module rather than a package.
for so in "$VENV_SP"/_yaml*.so; do [ -e "$so" ] && cp "$so" "$PAYLOAD_DIR/"; done

# STRIP THE BUNDLED JARS. flink_agents/lib/ holds all six per-minor dist jars (213MB, the
# common jar alone is 205MB) and the workers do not read them: on the TaskManager the jars
# arrive as YARN local resources via pipeline.jars, attached client-side by
# attach_flink_agents_jars(). Shipping them in the zip would add 205MB to every submission
# to no effect.
rm -rf "$PAYLOAD_DIR/flink_agents/lib"
find "$PAYLOAD_DIR" -name '__pycache__' -type d -prune -exec rm -rf {} + 2>/dev/null || true
# Zip from INSIDE the payload dir so the packages are the zip's own top-level entries.
( cd "$PAYLOAD_DIR" && zip -qr "$PYFS_ZIP" . -x '.*' )
echo "    payload : $(du -h "$PYFS_ZIP" | cut -f1), roots: $(unzip -Z1 "$PYFS_ZIP" | awk -F/ '{print $1}' | sort -u | tr '\n' ' ')"

# --- Cross-check the bundle's Flink minor against the cluster -------------------
# jars/ is read for METADATA ONLY — see the note on pipeline.jars below. The jars that
# actually ship are the copies inside the venv's site-packages, attached by the entry
# script. What matters here is catching a bundle built for the wrong Flink minor BEFORE
# burning a YARN submission, since the flink-agents dist jar is per-Flink-minor.
[ -d jars ] || fail "jars/ not found. Did you scp the whole dist/csa/ directory?"
COMMON_JAR="$( { ls jars/flink-agents-dist-common-*.jar 2>/dev/null || true; } | head -1)"
[ -n "$COMMON_JAR" ] || fail "No flink-agents-dist-common jar in jars/: $(ls jars/)"
# The bundle's target Flink minor is whatever the thin jar was built for.
FLINK_MINOR="$(ls jars/ \
  | sed -n 's/^flink-agents-dist-flink-\([0-9][0-9]*\.[0-9][0-9]*\)-.*-thin\.jar$/\1/p' \
  | head -1)"
[ -n "$FLINK_MINOR" ] || fail "Could not determine the Flink minor from jars/: $(ls jars/)"

# Cross-check the bundle against the cluster before burning a YARN submission.
CLUSTER_MINOR="$(basename "$DIST_JAR" | sed -n 's/.*flink-dist\(_[0-9.]*\)\{0,1\}-\([0-9][0-9]*\)\.\([0-9][0-9]*\).*/\2.\3/p')"
if [ -n "$CLUSTER_MINOR" ] && [ "$CLUSTER_MINOR" != "$FLINK_MINOR" ]; then
  fail "Version mismatch: bundle targets Flink $FLINK_MINOR, cluster runs $CLUSTER_MINOR.
The Flink Agents dist jar is per-Flink-minor. Rebuild the JARS with:
  FLINK_MAJOR_MINOR=$CLUSTER_MINOR scripts/build_csa_bundle.sh"
fi
echo "    bundle Flink minor: $FLINK_MINOR (cluster: ${CLUSTER_MINOR:-unknown})"

ENTRY="${ENTRY:-run_workflow_cluster_csa.py}"
JOB_NAME="${JOB_NAME:-ratatoskr-workflow-counter}"
[ -f "$ENTRY" ] || fail "Entry script not found: $ENTRY"

# RATATOSKR_SITE_PACKAGES tells the CLIENT-side driver where to find the agents jars to
# attach. Now that -Dpipeline.jars is gone, this is the ONLY route by which those jars
# reach the job — not belt-and-braces. The entry script also derives it from the installed
# flink_agents package, so it works if this is unset, but keep them agreeing: if this
# pointed at a different env than $VENV, the jars attached would not be the ones whose
# versions were cross-checked above.
export RATATOSKR_SITE_PACKAGES="$("$VENV/bin/python" -c 'import sysconfig;print(sysconfig.get_paths()["purelib"])')"
export PYFLINK_CLIENT_EXECUTABLE="$PWD/$VENV/bin/python"

# The CLIENT's route to ratatoskr/ and examples/. This used to come for free from -pyfs,
# which put the old two-rooted agentcode.zip on sys.path; now that the zip has a single
# `agentcode/` root, its contents sit one level below where zipimport would look. Point the
# client at the extracted directory instead of relying on Flink's zip path handling — and at
# client/, never agentcode/, for the shadowing reason given above.
export PYTHONPATH="$CODE_DIR${PYTHONPATH:+:$PYTHONPATH}"

# --- Assemble the submit command ----------------------------------------------
# -t yarn-per-job is deprecated in Flink 1.20 but still functional. It is the right
# choice for a first run because local -py/-pyfs paths work without staging
# everything to HDFS first. Move to `run-application -t yarn-application` once the
# job runs at all.
#
# ############################################################################
# # IT IS -yD ON THIS CLUSTER, NOT -D. EVERY -D IS SILENTLY DISCARDED.        #
# ############################################################################
#
# This cost more debugging time than every other problem here combined, because the
# failure is perfectly silent: no warning, no "unknown option", no rejected value. The
# flags simply never reach the JobManager, and you diagnose the consequences of defaults
# you believe you overrode.
#
# Mechanism, established by reading the classes in the parcel rather than guessing:
#
#   1. Cloudera Manager writes `execution.target: yarn-per-job` into
#      /etc/flink/conf/flink-conf.yaml. (Verified present, with that value.)
#   2. CliFrontend picks the first CustomCommandLine whose isActive() returns true.
#      FlinkYarnSessionCli.isActive() tests, among other things,
#        YarnJobClusterExecutor.NAME.equals(configuration.get(DeploymentOptions.TARGET))
#      against the FILE configuration — i.e. against that very key. So on CSA it is
#      active on every single submission, regardless of what you pass on the CLI.
#   3. It is registered BEFORE GenericCLI, so it wins. Confirmed from `flink run --help`,
#      which prints each CLI's section in registration order: "Options for yarn-cluster
#      mode" appears at line 160, "Options for Generic CLI mode" at line 193.
#   4. FlinkYarnSessionCli reads dynamic properties from its OWN option, `-yD`. It has no
#      -D handling at all. GenericCLI's DynamicPropertiesUtil.encodeDynamicProperties()
#      — the thing that consumes -D — never runs.
#   5. -D is still a *defined* option, so commons-cli parses it happily and nothing
#      complains. `flink run -t yarn-per-job -D` errors with "Missing argument for
#      option: D", proving the option exists; the active CLI just never reads its value.
#
# Proven both directions on a plain Java job (examples/streaming/WordCount.jar — no
# Python, no agents jars, so this is not a PyFlink argument-ordering quirk):
#   -D  taskmanager.numberOfTaskSlots=3  -> JM loaded 1, TM registered numSlots=1
#   -yD taskmanager.numberOfTaskSlots=3  -> JM loaded 3, TM registered numSlots=3
# Same result for a marker value in classloader.parent-first-patterns.additional: absent
# from the whole aggregated log with -D, present 6x with -yD.
#
# Three dead ends ruled out before finding it, recorded so nobody re-walks them:
#   - NOT the Cloudera Manager wrapper. /usr/bin/flink -> alternatives -> the parcel's
#     bin/flink; flink-exec-env.sh only exports env vars; the parcel's bin/flink ends in a
#     stock `exec ... CliFrontend "$@"` with no argument manipulation.
#   - NOT flink-conf.yaml overriding the CLI. Two of the three probe keys
#     (classloader.parent-first-patterns.additional, yarn.provided.lib.dirs) are absent
#     from that file entirely, yet both -D values vanished.
#   - NOT visible via yarn.application.name, which was the misleading first probe:
#     YarnClusterDescriptor.deployJobCluster() hardcodes the literal "Flink per-job
#     cluster" as the application name in per-job mode, so that key is ignored no matter
#     how it is passed. (`strings` on YarnClusterDescriptor.class shows all three
#     literals.) It is omitted below for that reason — do not re-add it, and do not
#     expect `yarn application -list` to show $JOB_NAME.
FLINK_CMD=(
  "$FLINK_BIN" run
  -t yarn-per-job
  -d
  # RUNAWAY GUARD. Learned the hard way: a JobManager that dies on a repeatable trigger
  # (see the AWS SDK note below) restarted 50+ times before anyone noticed, because
  # yarn.resourcemanager.am.max-attempts=2 was not the binding limit. Flink's
  # yarn.application-attempt-failures-validity-interval defaults to 10000ms, and YARN
  # forgets a failure once it is older than that window — so any crash loop slower than
  # 10s per cycle never accumulates toward the cap and retries forever. -1 disables the
  # window so every failure counts and the cap actually caps.
  #
  # Note these were passed as -D for the whole crash-loop investigation and therefore
  # never applied — /etc/flink/conf/flink-conf.yaml:48 sets yarn.application-attempts: 5,
  # which is what was actually in force while the loop ran.
  -yD yarn.application-attempts="${YARN_ATTEMPTS:-2}"
  -yD yarn.application-attempt-failures-validity-interval=-1
  # Two unrelated classloader problems, one flag.
  #
  # 1. pemja — give it a single class identity across the PyFlink task classloader and
  #    the Flink Agents action-operator classloader (FLINK-39226).
  #
  # 2. software.amazon.awssdk — WITHOUT THIS THE JOBMANAGER DIES IN A RESTART LOOP, and the
  #    symptom points nowhere near the cause. flink-agents-dist-common bundles an entire AWS
  #    SDK v2 (4128 classes, measured — presumably for Bedrock). It rides in on pipeline.jars,
  #    which is a CHILD-FIRST classloader, so its SDK shadows the platform's. Cloudera's
  #    Ranger RAZ S3 signer (ranger-raz-hook-s3) is compiled against the platform's newer SDK
  #    and calls a method the bundled one does not have:
  #
  #      java.lang.NoSuchMethodError: Checksummer.forFlexibleChecksum(
  #          String, ChecksumAlgorithm, PayloadChecksumStore)
  #        at org.apache.ranger.raz.hook.s3.RazS3SignerPlugin.addCheckSumToRequest(...:163)
  #
  #    Verified with javap on both jars rather than inferred:
  #      agents   jar: forFlexibleChecksum(ChecksumAlgorithm) / (String, ChecksumAlgorithm)
  #      platform jar: the same two, each PLUS a PayloadChecksumStore parameter
  #    The platform's only copy lives in lib/flink-s3-fs-hadoop-*.jar — i.e. already on the
  #    parent classloader — so parent-first is all that is needed to make RAZ bind correctly.
  #
  #    Why it presents as a mystery: the throw happens on `jobmanager-io-thread-N` while
  #    FINALIZING THE FIRST CHECKPOINT to s3a://. Flink's FatalExitExceptionHandler treats any
  #    uncaught exception on that thread as fatal and halts the JVM, so the AM exits 239, YARN
  #    restarts it, the job checkpoints again, and it dies again — ~22s per cycle, forever.
  #    (50+ attempts observed. yarn.resourcemanager.am.max-attempts=2 does NOT stop it: Flink
  #    sets a 10s attempt-failure validity interval, and 22s > 10s resets the counter each
  #    time.) Nothing in the loop mentions Python, agents, or the actual bad jar.
  #
  #    Parent-first still falls back to the child jar for classes the parent lacks (standard
  #    ClassLoader.loadClass delegation), so this does not strip the bundled SDK — it only
  #    stops it winning where both sides define the same class.
  #
  #    RETRACTED: an earlier version of this comment claimed "adding software.amazon.awssdk
  #    did NOT by itself fix the crash loop; the actual cause was the system classpath ORDER."
  #    That conclusion was unfounded. Both this flag and the include-user-jar flag below were
  #    passed as -D throughout, so NEITHER was ever in effect — no attribution between them
  #    was possible. The reasoning that follows (system classpath order) is still sound on the
  #    evidence of the JM's own logged Classpath: line, but which flag is load-bearing is
  #    genuinely untested until a -yD submission settles it.
  -yD classloader.parent-first-patterns.additional='pemja;software.amazon.awssdk'
  # THE ACTUAL FIX for the NoSuchMethodError above.
  #
  # In yarn-per-job mode Flink copies pipeline.jars onto the JobManager's SYSTEM classpath,
  # positioned by yarn.per-job-cluster.include-user-jar, whose default is ORDER — meaning
  # "sorted in with Flink's own jars". Measured from the JM's own logged Classpath: line:
  #
  #   position  5  flink-agents-dist-common-0.3-SNAPSHOT.jar   <-- bundles AWS SDK v2
  #   position 32  lib/flink-s3-fs-hadoop-1.20.5-csa1.18.0.0.jar
  #   position 45  lib/ranger-raz-hook-s3-2.8.0.7.3.2.20000-258.jar
  #
  # "flink-agents" sorts before "flink-s3", so the agents jar's older AWS SDK lands FIRST and
  # wins on the system classloader, where no parent-first pattern can reach it. LAST appends
  # the user jars after Flink's own, so the platform SDK that RAZ was compiled against wins.
  #
  # LAST rather than DISABLED deliberately: the agents common jar still needs to be visible to
  # the JobManager's system classloader for the JM-side CompileUtils path (the same reason
  # install_flink_agents_common_lib_jar() exists on the Docker path). DISABLED would keep it
  # out of the system classpath entirely and risk trading this failure for that one.
  #
  # MIND THE KEY NAME. `yarn.per-job-cluster.include-user-jar` is the DEPRECATED alias and was
  # silently ignored here — passing it left the JM classpath byte-for-byte unchanged (verified
  # by re-reading the JM's logged Classpath: line, agents jar still at position 5) and the
  # string never appeared in the JM's effective config. The live key, read out of
  # YarnConfigOptions in flink-dist_2.12-1.20.5-csa1.18.0.0.jar, is:
  -yD yarn.classpath.include-user-jar=LAST
  # WHAT TRIGGERS THE CHECKPOINT — corrected. An earlier version of this comment claimed the
  # crash was the FINAL checkpoint of a bounded source on a job that had already succeeded,
  # and that the three expected records proved 16 successful runs with a fatal epilogue. That
  # story was wrong in its central claim and is retracted:
  #
  #   - `switched from RUNNING to FINISHED` appears ZERO times in app 0009's full aggregated
  #     log (4.8MB, all attempts). No task ever finished, so there was no post-finish final
  #     checkpoint to blame. The records appear because from_collection drains in
  #     milliseconds — they are emitted BEFORE the crash, not by a completed job.
  #   - The JM's own log names the trigger: `JobMaster - Triggering a manual checkpoint for
  #     job ...`, ~400ms after the operator reached RUNNING, then `CheckpointCoordinator -
  #     Triggering checkpoint 1`.
  #
  # Periodic checkpointing IS enabled cluster-wide — flink-conf.yaml sets
  # execution.checkpointing.interval: 600000 — but at 10 minutes it cannot be the trigger at
  # t+400ms. (A related self-inflicted red herring: the JM logs that value re-serialized as
  # `10 min`, and a careless grep that cut at the first space read it as "10". It is not 10ms.)
  # The client-side reading of getCheckpointInterval() as -1 was also misleading: the client
  # never loaded it; the JM gets it from the file.
  #
  # WHAT TRIGGERS THE "manual" CHECKPOINT IS STILL UNIDENTIFIED. Ruled out: the agents jars
  # (zero triggerCheckpoint references), flink_agents' own execute() (a bare
  # self.__env.execute(job_name=...)), and a REST call (no handler names in the log).
  #
  # It does not block the fix, and that is the point worth keeping: whatever fires it, the
  # write lands in state.checkpoints.dir — s3a://…/checkpoints — and dies in the RAZ signer.
  # The classpath fix above addresses the mechanism regardless of the trigger, which is why it
  # is the right fix rather than suppressing one particular trigger.
  #
  # Note jobmanager.archive.fs.dir is on s3a:// too, so job ARCHIVING would hit the identical
  # NoSuchMethodError at termination even if every checkpoint were suppressed. Another reason
  # to repair the classpath rather than dodge the checkpoint.
  #
  # Kept, but now understood as belt-and-braces rather than the fix, and inert for the later
  # Kafka milestone since unbounded sources never finish:
  -yD execution.checkpointing.checkpoints-after-tasks-finish.enabled=false
  # USE -pyexec/-pyfs, NOT -Dpython.executable/-Dpython.files. Measured, not assumed:
  # with the -D form, `yarn logs -applicationId ...` (780KB, both JM and TM) contained
  # ZERO occurrences of `python.executable` or `python.archives`.
  #
  # Two independent reasons, and at the time this was written only the second was known:
  #   1. On THIS cluster -D is discarded wholesale, for every key — see the -yD banner above.
  #      That alone explains the zero occurrences.
  #   2. Independently of that, the Python dependency options have their own dedicated flags,
  #      parsed by CliFrontend/PythonDependencyUtils and merged into the configuration handed
  #      to PythonDriver. This is why the dedicated flags are still the right answer here
  #      rather than switching them to -yD: keep them portable to clusters where -D works.
  #
  # The failure this produces is doubly misleading: no "unknown option", no "interpreter
  # not found" — just the default bare `python` being used, and a TaskManager dying with
  # `ModuleNotFoundError: No module named 'pyflink'`, which reads as if the NODE lacked
  # PyFlink when in fact the node's PyFlink was never consulted.
  #
  # THE WORKER INTERPRETER IS THE NODE'S, NOT A SHIPPED ONE. This is the crux of the whole
  # CSA approach and the one place the venv strategy nearly came apart.
  #
  # Pointing -pyexec at an interpreter inside a python.archives tarball fails at CPython
  # bootstrap:
  #
  #     PYTHONHOME = .../python-archives/venv/agentvenv
  #     stdlib dir = .../python-archives/venv/agentvenv/lib64/python3.11
  #     Fatal Python error: init_fs_encoding: failed to get the Python codec of the
  #                         filesystem encoding
  #     ModuleNotFoundError: No module named 'encodings'
  #
  # Flink sets PYTHONHOME to the extracted archive root when the interpreter lives inside
  # it, because python.archives is designed for a SELF-CONTAINED Python (hence conda-pack
  # in the upstream docs). A `python -m venv --system-site-packages` tree has no stdlib of
  # its own at all — `encodings` is in /usr/lib64/python3.11 — so forcing PYTHONHOME there
  # leaves CPython unable to start. Not a relocation bug: the venv is *deliberately* not
  # self-contained, which is exactly what makes it 13MB instead of 626MB.
  #
  # So use the node's own interpreter. It already has the stdlib, Cloudera's PyFlink 1.20.5
  # and the pemja 0.5.7 that matches the parcel's flink-python jar — none of which we could
  # ship correctly anyway — and the libraries it lacks ride in via -pyfs below.
  -pyexec "$SYS_PY"
  -pyclientexec "$PWD/$VENV/bin/python"
  # NO -Dpipeline.jars HERE. This is deliberate and was measured, not assumed.
  #
  # The entry script calls patch_flink_agents_jar_loading(), whose attach_flink_agents_jars()
  # does ONE env.add_jars(common, thin) using the copies inside the venv's site-packages.
  # PyFlink's add_jars APPENDS to whatever pipeline.jars already holds. So passing the
  # jars/ copies here too produced FOUR entries — the same two jars via two different
  # paths — verified on the gateway:
  #
  #     .../jars/flink-agents-dist-common-0.3-SNAPSHOT.jar
  #     .../jars/flink-agents-dist-flink-1.20-0.3-SNAPSHOT-thin.jar
  #     .../site-packages/flink_agents/lib/common/flink-agents-dist-common-0.3-SNAPSHOT.jar
  #     .../site-packages/flink_agents/lib/flink-1.20/...-thin.jar
  #     -> 430MB uploaded per submission (the common jar is 205MB, sent twice)
  #
  # The byte count is the lesser problem. Two copies of the same classes in the user
  # classloader is exactly the class-identity condition that the single-add_jars
  # arrangement exists to prevent (FLINK-39226, pemja.core.object.PyObject
  # ClassCastException) — so this "belt and braces" was quietly loosening the belt.
  # Leaving attach_flink_agents_jars() as the single authority gives 2 entries, 205MB,
  # and one class identity.
  #
  # The jars/ directory is still required, but only as METADATA: FLINK_MINOR is derived
  # from the thin jar's name and cross-checked against the parcel's flink-dist jar above.
  # The jars that actually ship come from the venv.
  #
  # Whatever is added must stay in pipeline.jars and never reach the system classpath:
  # the thin jar references Jackson classes that live in the common jar, so a thin jar
  # alone on the parent classloader makes to_datastream() die with
  # NoClassDefFoundError: com.fasterxml.jackson.core.JsonProcessingException.
  "${KERBEROS_OPTS[@]+"${KERBEROS_OPTS[@]}"}"
  -pyfs "$PYFS_ZIP"
  -py "$PWD/$ENTRY"
)

log "Submit command"
printf '    %q \\\n' "${FLINK_CMD[@]}" | sed '$ s/ \\$//'

if [ "${DRY_RUN:-0}" = "1" ]; then
  log "DRY_RUN=1 — not submitting."
  exit 0
fi

log "Submitting"
"${FLINK_CMD[@]}"

cat <<EOF

==> Submitted. To inspect:

    # Do NOT grep for the job name: in yarn-per-job mode YarnClusterDescriptor hardcodes
    # the application name, so every submission appears as "Flink per-job cluster".
    yarn application -list | grep 'Flink per-job cluster'
    yarn logs -applicationId <appId> | grep doubled    # expects input/doubled records

    # While an app is RUNNING, log aggregation retains only the live container, so a
    # crash-looped app shows one attempt. Kill it first, then aggregate, to see them all.

Expect: {'input': 5, 'doubled': 10, 'agent': 'workflow_counter'} and 10/20, 15/30.

If it failed, the symptom table in the runbook maps each likely error to its fix. One
symptom worth knowing in advance, because it looks like a broken archive and is not:
    ImportError: ..._pydantic_core...so: failed to map segment from shared object
means the venv was extracted onto a NOEXEC filesystem. /tmp is mounted noexec on these
nodes. Check that yarn.nodemanager.local-dirs is on an exec-capable mount.
EOF
