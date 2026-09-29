#!/usr/bin/env bash
# Phase 0: collect every fact needed to build and submit the Flink Agents bundle,
# in ONE run, from the CSA DataHub gateway node.
#
#   scp scripts/probe_csa_gateway.sh <user>@<gateway>:~/
#   ssh <user>@<gateway> 'bash ~/probe_csa_gateway.sh' | tee csa-probe.txt
#
# STRICTLY READ-ONLY. It runs no kinit, creates no HDFS directories, writes nothing
# outside stdout, and changes no cluster state — so it is safe to run repeatedly and
# safe to run against a cluster someone else owns.
#
# Deliberately NOT `set -e`: every check is independent and individually tolerant of
# failure, because the point is to come back with the complete picture from a single
# SSH round-trip. Stopping at the first missing tool would mean one question answered
# per round-trip, which is exactly the loop this script exists to avoid.

section() { printf '\n========== %s ==========\n' "$*"; }
note()    { printf '    %s\n' "$*"; }

printf 'CSA gateway probe — %s on %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$(hostname -f 2>/dev/null || hostname)"

section "1. OS / arch / glibc  (determines the bundle build base)"
sed -n 's/^PRETTY_NAME=//p' /etc/os-release 2>/dev/null | tr -d '"'
note "arch:  $(uname -m)"
note "glibc: $(ldd --version 2>/dev/null | head -1)"
# The bundle is built on Rocky 8 (glibc 2.28) on purpose: glibc is backward
# compatible, so it loads on 2.34 (RHEL9) too, and one artifact serves both.
# A glibc OLDER than the build base is the failure case worth knowing about.

section "2. Java"
java -version 2>&1 | head -2 || note "java not on PATH"

section "3. Flink parcel"
# Same candidate list as scripts/submit_agent_csa.sh, so the two cannot disagree.
PARCEL_CANDIDATES=(
  "/opt/cloudera/parcels/FLINK/lib/flink"
  "/opt/cloudera/parcels/CSA/lib/flink"
  "/opt/cloudera/parcels/FLINK-1.20.1/lib/flink"
)
FOUND=""
for c in "${PARCEL_CANDIDATES[@]}"; do
  [ -d "$c/lib" ] && FOUND="$c" && break
done
if [ -z "$FOUND" ]; then
  note "NOT FOUND in the expected locations. All parcels present:"
  ls -d /opt/cloudera/parcels/*/ 2>/dev/null | sed 's/^/        /' || note "no /opt/cloudera/parcels"
  note "Find it with: ls -d /opt/cloudera/parcels/*/lib/flink"
else
  note "FLINK_HOME=$FOUND"
  note "dist jar: $(basename "$(ls "$FOUND"/lib/flink-dist*.jar 2>/dev/null | head -1)" 2>/dev/null)"
  note "The dist jar filename carries the Flink minor. It MUST be 1.20 to match the"
  note "bundle: flink-agents release-0.3 ships dist modules for 1.20, 2.0-2.3 only,"
  note "so a 1.19 or 1.18 parcel has no usable jar at all."
fi

section "4. *** DECISIVE GO/NO-GO: does the parcel ship PyFlink? ***"
# Cloudera does not support PyFlink on CSA, and the parcel may be built without the
# Python integration. Without flink-python there is no PythonDriver to launch and no
# amount of bundling helps — the fallback is the CSA-Operator-on-AKS path.
# Check lib/ AND opt/. In vanilla Flink flink-python lives in opt/ and you copy it into
# lib/ to enable PyFlink; Cloudera ships it ALREADY IN lib/, so it is on the system
# classpath by default. An earlier version of this check looked only in opt/ and
# reported NO-GO against a CSA parcel that fully supports PyFlink — the conclusion was
# wrong, not the cluster. Never narrow this back to one directory.
if [ -n "$FOUND" ]; then
  PYJAR="$(ls "$FOUND"/lib/flink-python*.jar "$FOUND"/opt/flink-python*.jar 2>/dev/null | head -1)"
  if [ -n "$PYJAR" ]; then
    note "GO — flink-python jar present:"
    ls -la "$PYJAR" | sed 's/^/        /'
    case "$PYJAR" in
      */lib/*) note "in lib/ — already on the system classpath (Cloudera's layout)" ;;
      */opt/*) note "in opt/ ONLY — vanilla layout; it is NOT on the classpath until it"
               note "is in lib/, which the read-only parcel dir may prevent." ;;
    esac
  else
    note "NO-GO — no flink-python jar in either $FOUND/lib/ or $FOUND/opt/"
    note "Contents of opt/:"; ls "$FOUND/opt/" 2>/dev/null | sed 's/^/        /'
    note "python-related in lib/:"; ls "$FOUND/lib/" 2>/dev/null | grep -i python | sed 's/^/        /'
  fi
  # CSA also ships the matching PyFlink WHEEL SET here. This is the authoritative
  # source for every runtime pin: build the shipped env from these wheels rather than
  # resolving from PyPI, because Cloudera patches the dependency set (their
  # apache-flink 1.20.5 requires pemja 0.5.7, where upstream 1.20.1 pins pemja 0.4.1 —
  # a mismatch that resolves cleanly and then dies in a TaskManager).
  WHEELDIR="$FOUND/lib/flink-python-source"
  if [ -d "$WHEELDIR" ]; then
    note "--- parcel wheel set: $WHEELDIR ($(ls "$WHEELDIR"/*.whl 2>/dev/null | wc -l | tr -d ' ') wheels) ---"
    note "PIN THE BUNDLE TO THESE:"
    ls "$WHEELDIR" 2>/dev/null \
      | grep -iE "^(apache_flink|apache_beam|pemja|numpy|pyarrow|pandas|protobuf|setuptools|py4j)-" \
      | sed 's/^/        /'
  else
    note "(no flink-python-source wheel dir — pins must come from the wheel METADATA)"
  fi
  note "--- all python-related entries in opt/ and lib/ ---"
  ls "$FOUND"/opt/ "$FOUND"/lib/ 2>/dev/null | grep -i python | sed 's/^/        /' \
    || note "(none)"
else
  note "skipped — parcel not located"
fi

section "5. Python interpreters on the node"
# Informational only. The bundle ships its own interpreter and libpython precisely so
# that the node's Python does not matter; a mismatch here is NOT a blocker.
note "python3: $(python3 --version 2>&1)"
ls /usr/bin/python3.* 2>/dev/null | sed 's/^/        /' || note "no /usr/bin/python3.*"

section "6. Kerberos (read-only — no kinit)"
klist 2>&1 | head -5 || note "no ticket cache; run 'kinit <user>' before submitting"

section "7. YARN"
yarn node -list 2>/dev/null | head -12 || note "yarn not on PATH or not reachable"

section "8. HDFS home directory"
hdfs dfs -ls "/user/$(whoami)" 2>&1 | head -8 \
  || note "cannot list /user/$(whoami) — may need creating, or needs a Kerberos ticket"

section "9. Disk space for the bundle (~1-2GB unpacked)"
df -h "$HOME" 2>/dev/null | tail -1

section "SUMMARY — what to report back"
cat <<'EOF'
    1. Section 4 GO or NO-GO           (decides whether this approach is viable at all)
    2. Section 3 dist jar version      (must be flink-dist-1.20.x)
    3. Section 1 OS / arch / glibc     (confirms the Rocky 8 build base is right)
    4. Section 7/8 YARN + HDFS reachable
EOF
