"""
Shared helpers for submitting PyFlink jobs to the Docker Compose Flink cluster.

Jobs must be submitted with ``flink run`` from the JobManager container so they
appear in the Web UI (http://localhost:8081).
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Set

# Flink install root. Defaults to the Docker Compose image layout (``/opt/flink``);
# override via ``FLINK_HOME`` for cluster deployments where Flink lives elsewhere
# (e.g. a Cloudera CSA parcel at
# ``/opt/cloudera/parcels/FLINK/lib/flink``). ``SITE_PACKAGES`` and
# ``FLINK_AGENTS_SRC`` are separately overridable because on a parcel install the
# Flink root is read-only, so the agent runtime has to live outside it.
FLINK_HOME = Path(os.environ.get("FLINK_HOME") or "/opt/flink")
FLINK_LIB = FLINK_HOME / "lib"
FLINK_BIN = FLINK_HOME / "bin" / "flink"
SITE_PACKAGES = Path(
    os.environ.get("RATATOSKR_SITE_PACKAGES")
    or FLINK_HOME / "pythonpath" / "agent-site-packages"
)
FLINK_AGENTS_SRC = Path(
    os.environ.get("RATATOSKR_FLINK_AGENTS_SRC") or FLINK_HOME / "flink-agents"
)

DEFAULT_PYTHONPATH = os.pathsep.join(
    str(p)
    for p in (
        FLINK_HOME,
        SITE_PACKAGES,
        FLINK_HOME / "opt" / "python" / "pyflink",
        FLINK_HOME / "opt" / "python" / "py4j",
    )
)


def ensure_python_symlink() -> None:
    python3 = Path("/usr/bin/python3")
    python = Path("/usr/bin/python")
    if python3.is_file() and not python.exists():
        subprocess.run(["ln", "-sf", str(python3), str(python)], check=False)


# Matches the ``<major>.<minor>`` in a flink-dist jar name, tolerating an optional
# Scala suffix and vendor qualifiers, e.g.::
#     flink-dist-2.1.3.jar                            -> 2.1
#     flink-dist-1.20.1.jar                           -> 1.20
#     flink-dist_2.12-1.20.1-csa1.14.0.0-cdh7.3.2.jar -> 1.20
# Naive prefix-stripping mis-parses the Scala-suffixed form that Cloudera parcels
# may ship, yielding a bogus version that then corrupts every version-derived path.
_FLINK_DIST_VERSION_RE = re.compile(r"flink-dist(?:_[\d.]+)?-(\d+)\.(\d+)")


def flink_major_version() -> str:
    jars = sorted(FLINK_LIB.glob("flink-dist*.jar"))
    if not jars:
        raise FileNotFoundError(f"No flink-dist jar in {FLINK_LIB}")
    for jar in jars:
        match = _FLINK_DIST_VERSION_RE.search(jar.name)
        if match:
            return f"{match.group(1)}.{match.group(2)}"
    raise ValueError(
        f"Could not parse a Flink version from: {', '.join(j.name for j in jars)}"
    )


def ensure_flink_agents_jars() -> None:
    """Copy Flink Agents dist JARs into the Python package lib tree for workers."""
    import shutil

    if not FLINK_AGENTS_SRC.is_dir():
        return

    lib_root = SITE_PACKAGES / "flink_agents" / "lib"
    flink_major = flink_major_version()
    common_dir = lib_root / "common"
    version_dir = lib_root / f"flink-{flink_major}"

    common_jar = next(
        (FLINK_AGENTS_SRC / "dist/common/target").glob("flink-agents-dist-common-*.jar"),
        None,
    )
    thin_jars = list(
        (FLINK_AGENTS_SRC / f"dist/flink-{flink_major}/target").glob(
            f"flink-agents-dist-flink-{flink_major}-*-thin.jar"
        )
    )
    if not common_jar or not thin_jars:
        return

    for directory in (lib_root, common_dir, version_dir):
        directory.mkdir(parents=True, exist_ok=True)
        init_py = directory / "__init__.py"
        if not init_py.exists():
            init_py.touch()

    shutil.copy2(common_jar, common_dir / common_jar.name)
    shutil.copy2(thin_jars[0], version_dir / thin_jars[0].name)


def install_flink_agents_common_lib_jar() -> None:
    """Install common JAR on ``/opt/flink/lib`` for JM-side CompileUtils (parent classloader)."""
    import shutil

    common_dir = SITE_PACKAGES / "flink_agents" / "lib" / "common"
    if not common_dir.is_dir():
        return
    for jar in common_dir.glob("*.jar"):
        target = FLINK_LIB / jar.name
        if target.exists() and target.stat().st_size == jar.stat().st_size:
            continue
        try:
            shutil.copy2(jar, target)
        except OSError:
            continue


def ensure_pemja_parent_classpath() -> None:
    """Place ``flink-python`` (Pemja classes) on the parent/app classpath.

    ``pemja.core.object.PyObject``/``PyIterator`` ship inside ``flink-python`` under
    ``/opt/flink/opt`` (not on the default classpath). When both the PyFlink task
    classloader and the Flink Agents action-operator classloader load Pemja from
    their own user jars, the two class identities differ and casts fail with a
    ``ClassCastException`` (FLINK-39226). Copying the jar into ``/opt/flink/lib`` so
    the app classloader owns Pemja, combined with
    ``classloader.parent-first-patterns.additional: pemja``, gives one shared
    identity across all child-first classloaders.

    Note: the jar must be present before the JVM starts, so callers should restart
    the JobManager/TaskManager after this runs (the Studio restart flow does).
    """
    import shutil

    opt_dir = FLINK_HOME / "opt"
    for jar in sorted(opt_dir.glob("flink-python-*.jar")):
        target = FLINK_LIB / jar.name
        if target.exists() and target.stat().st_size == jar.stat().st_size:
            continue
        try:
            shutil.copy2(jar, target)
        except OSError:
            continue


def remove_flink_agents_lib_jars() -> None:
    """Remove all Flink Agents JARs from ``/opt/flink/lib``.

    The image copies both common and thin into ``lib``. Historically bootstrap only
    deleted the common JAR (Pemja / dual-classloader fix), which left thin alone on
    the parent classloader. Thin references Jackson classes that live in common →
    ``NoClassDefFoundError: com.fasterxml.jackson.core.JsonProcessingException``
    during ``to_datastream()``.

    Pipelines must load common+thin together via ``attach_flink_agents_jars`` (one
    user classloader). Keep them out of ``/opt/flink/lib`` after bootstrap.
    """
    for jar in FLINK_LIB.glob("flink-agents-*.jar"):
        try:
            jar.unlink()
        except OSError:
            pass


def remove_flink_agents_common_from_classpath() -> None:
    """Backward-compatible alias: strip all Agents JARs from ``/opt/flink/lib``."""
    remove_flink_agents_lib_jars()


def ensure_flink_agents_common_on_classpath() -> None:
    """Alias for ``install_flink_agents_common_lib_jar``."""
    install_flink_agents_common_lib_jar()


def flink_agents_jar_uris(*, pipeline: bool = False) -> list[str]:
    """Return Flink Agents dist JAR URIs.

    For ``pipeline=True``, attach only the thin Flink-version JAR via
    ``AgentsExecutionEnvironment.get_execution_environment(env)`` — do not also call
    ``attach_flink_agents_jars`` or copy common JAR into ``/opt/flink/lib``.
    """
    flink_major = flink_major_version()
    common_dir = SITE_PACKAGES / "flink_agents" / "lib" / "common"
    version_dir = SITE_PACKAGES / "flink_agents" / "lib" / f"flink-{flink_major}"
    if pipeline:
        jars = sorted(version_dir.glob("*-thin.jar"))
    else:
        jars = sorted(common_dir.glob("*.jar")) + sorted(version_dir.glob("*-thin.jar"))
    return [f"file://{jar.resolve()}" for jar in jars]


def attach_flink_agents_jars(stream_env) -> None:
    """Attach Flink Agents JARs in one user classloader (common + thin together).

    Flink Agents' built-in loader calls ``add_jars`` once per JAR, which splits Pemja
    across classloaders on TaskManagers. A single ``add_jars(*uris)`` avoids that.
    """
    jar_uris = flink_agents_jar_uris(pipeline=False)
    if not jar_uris:
        return
    joined = ";".join(jar_uris)
    try:
        stream_env.get_config().set("pipeline.jars", joined)
    except Exception:
        # Not available on every PyFlink: on 1.20 this raises AttributeError, because
        # get_config() returns an ExecutionConfig with no .set(). add_jars below is what
        # actually does the work; this is a no-op there, kept for versions where it helps.
        pass

    # add_jars is the load-bearing call, and its failure mode used to be invisible: this
    # was `except Exception: pass`, which on CSA meant ZERO jars attached and a
    # ClassNotFoundException hours later pointing nowhere near the cause. The specific
    # trap: add_jars -> add_jars_to_context_class_loader reflects on
    # URLClassLoader.addURL, which on Java 17 needs
    # --add-opens=java.base/java.net=ALL-UNNAMED. CSA supplies that via env.java.opts.all
    # in flink-conf.yaml, but ONLY if FLINK_CONF_DIR points at /etc/flink/conf — the
    # parcel has no conf/ of its own, so an unset FLINK_CONF_DIR silently removes it.
    #
    # Still tolerate the exception rather than re-raising: on some versions the set()
    # above has already done the job, so raising eagerly would break a working path.
    # Instead verify the effective config and only fail if the jars really are absent.
    add_jars_error: Exception | None = None
    try:
        stream_env.add_jars(*jar_uris)
    except Exception as exc:
        add_jars_error = exc
    if add_jars_error is None:
        return

    try:
        from pyflink.util.java_utils import get_j_env_configuration

        effective = get_j_env_configuration(
            stream_env._j_stream_execution_environment
        ).getString("pipeline.jars", "")
    except Exception:
        # Cannot verify — preserve the old tolerant behaviour rather than guessing.
        return

    import os

    if all(os.path.basename(uri) in effective for uri in jar_uris):
        return
    raise RuntimeError(
        "Failed to attach the Flink Agents jars to pipeline.jars: "
        f"{type(add_jars_error).__name__}: {add_jars_error}\n"
        f"Wanted: {joined}\nEffective pipeline.jars: {effective!r}\n"
        "If this is an InaccessibleObjectException about java.net, FLINK_CONF_DIR is "
        "probably unset, so --add-opens=java.base/java.net=ALL-UNNAMED from "
        "flink-conf.yaml never reached the JVM. On a CM-managed parcel install set:\n"
        "  export FLINK_CONF_DIR=/etc/flink/conf"
    ) from add_jars_error


PEMJA_VERSION = "pemja>=0.6.0,<0.7.0"


def ensure_pemja_embed_runtime() -> None:
    """Install Pemja + libpython for Flink Agents embedded Python on workers."""
    try:
        import pemja  # noqa: F401
    except ImportError:
        subprocess.run(
            [
                "python3",
                "-m",
                "pip",
                "install",
                "--target",
                str(SITE_PACKAGES),
                PEMJA_VERSION,
            ],
            check=False,
        )


def ensure_pyflink_beam_runtime() -> None:
    """Install PyFlink / Beam runner deps when missing from the image."""
    missing: list[str] = []
    try:
        import apache_beam  # noqa: F401
    except ImportError:
        missing.extend(
            [
                "numpy>=1.22.4,<2",
                "pyarrow>=5.0.0,<21.0.0",
                "apache-beam>=2.54.0,<=2.61.0",
                "setuptools>=75.3,<82",
            ]
        )
    try:
        import pemja  # noqa: F401
    except ImportError:
        missing.append(PEMJA_VERSION)
    # PyFlink 1.19+/2.x imports ``avro.errors`` (apache-avro >=1.11). The legacy
    # ``avro-python3`` wheel provides an ``avro`` package without that module and
    # breaks TaskManagers with ModuleNotFoundError during stage-bundle startup.
    try:
        from avro.errors import AvroTypeException  # noqa: F401
    except ImportError:
        missing.append("avro>=1.11.0,<1.12.0")
    if not missing:
        return

    subprocess.run(
        [
            "python3",
            "-m",
            "pip",
            "install",
            "--target",
            str(SITE_PACKAGES),
            "--upgrade",
            *missing,
        ],
        check=False,
    )


def bootstrap_cluster_runtime(
    *,
    download_kafka_jars: bool = False,
    install_agents_jars: bool = True,
) -> None:
    """Prepare Python workers and optional Flink Agents JARs."""
    ensure_python_symlink()
    ensure_pemja_parent_classpath()
    # Never leave thin-only Agents JARs on the parent classpath (Jackson CNF).
    remove_flink_agents_lib_jars()
    ensure_pemja_embed_runtime()
    ensure_pyflink_beam_runtime()
    if install_agents_jars:
        ensure_flink_agents_jars()


def bootstrap_cluster_containers(*, profile: str | None = None) -> None:
    """Sync Flink Agents JAR layout on JobManager and TaskManagers before submit."""
    from ratatoskr.constants import DEFAULT_PROFILE
    from ratatoskr.docker_utils import container_id, docker_exec

    active_profile = profile or DEFAULT_PROFILE

    command = (
        "cd /opt/flink && "
        "export PYTHONPATH=/opt/flink:/opt/flink/pythonpath/agent-site-packages:"
        "/opt/flink/opt/python/pyflink:/opt/flink/opt/python/py4j && "
        "python3 -c 'from ratatoskr.runtime.cluster_launch_test import bootstrap_runtime; "
        "bootstrap_runtime()'"
    )
    for service in ("jobmanager", "taskmanager"):
        cid = container_id(service, profile=active_profile)
        if cid:
            subprocess.run(
                ["docker", "exec", "-u", "root", cid, "bash", "-c", command],
                check=False,
            )


def rest_base(*, rest_port: int | None = None) -> str:
    from ratatoskr.flink_rest import default_flink_rest_port

    host = os.environ.get("FLINK_REST_ADDRESS", "localhost").strip()
    port = rest_port if rest_port is not None else default_flink_rest_port()
    return f"http://{host}:{port}"


def fetch_json(path: str, *, rest_port: int | None = None) -> dict:
    with urllib.request.urlopen(f"{rest_base(rest_port=rest_port)}{path}", timeout=10) as resp:
        return json.loads(resp.read().decode())


def wait_for_flink_rest(
    *,
    timeout_sec: float = 300,
    poll_interval_sec: float = 2.0,
) -> None:
    deadline = time.time() + timeout_sec
    last_error: Optional[str] = None
    while time.time() < deadline:
        try:
            fetch_json("/overview")
            print(f"Flink REST ready at {rest_base()}")
            return
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_error = str(exc)
        time.sleep(poll_interval_sec)
    raise TimeoutError(
        f"Flink REST not available at {rest_base()} after {timeout_sec}s: {last_error}"
    )


def parse_submitted_job_id(output: str) -> str:
    for line in output.splitlines():
        marker = "Job has been submitted with JobID "
        if marker in line:
            return line.split(marker, 1)[1].strip()
    raise RuntimeError(f"Could not parse JobID from flink CLI output:\n{output}")


def wait_for_job(
    job_id: str,
    *,
    accept: Optional[Set[str]] = None,
    reject: Optional[Set[str]] = None,
    timeout_sec: int = 120,
    poll_interval_sec: float = 1.0,
) -> str:
    accept = accept or {"FINISHED"}
    reject = reject or {"FAILED", "CANCELED"}
    deadline = time.time() + timeout_sec
    last_state = "UNKNOWN"

    while time.time() < deadline:
        try:
            detail = fetch_json(f"/jobs/{job_id}")
            last_state = detail.get("state", last_state)
            if last_state in accept:
                return job_id
            if last_state in reject:
                raise RuntimeError(
                    f"Flink job {job_id} ended with state {last_state}"
                )
        except urllib.error.URLError:
            pass
        time.sleep(poll_interval_sec)

    raise TimeoutError(
        f"Timed out waiting for job {job_id} (last state: {last_state})"
    )


def find_running_jobs(job_name: str) -> List[str]:
    try:
        overview = fetch_json("/jobs/overview")
    except urllib.error.URLError:
        return []
    out: List[str] = []
    for job in overview.get("jobs", []):
        if job.get("name") != job_name:
            continue
        if job.get("state") == "RUNNING":
            jid = job.get("jid")
            if jid:
                out.append(str(jid))
    return out


def cancel_job(job_id: str, *, rest_port: int | None = None) -> None:
    """Request cancellation of a Flink job via REST API."""
    url = f"{rest_base(rest_port=rest_port)}/jobs/{job_id}?mode=cancel"
    req = urllib.request.Request(url, method="PATCH")
    with urllib.request.urlopen(req, timeout=10):
        return


def flink_run_py(
    entry_script: Path,
    *,
    pyfiles: Optional[Sequence[Path]] = None,
    detached: bool = True,
    extra_args: Optional[Iterable[str]] = None,
    env: Optional[dict[str, str]] = None,
) -> tuple[str, str]:
    """Submit a PyFlink script via ``flink run``. Returns ``(job_id, cli_output)``."""
    if not FLINK_BIN.is_file():
        raise FileNotFoundError(f"Flink CLI not found at {FLINK_BIN}")
    if not entry_script.is_file():
        raise FileNotFoundError(f"Entry script not found: {entry_script}")

    cmd = [str(FLINK_BIN), "run"]
    host = os.environ.get("FLINK_REST_ADDRESS", "localhost").strip() or "localhost"
    port = os.environ.get("FLINK_REST_PORT", "8081").strip() or "8081"
    cmd.extend(["-m", f"{host}:{port}"])
    if detached:
        cmd.append("-d")
    if pyfiles:
        uris = ",".join(f"file://{p.resolve()}" for p in pyfiles)
        cmd.extend(["-pyFiles", uris])
    cmd.extend(["-py", str(entry_script.resolve())])
    if extra_args:
        cmd.extend(extra_args)

    run_env = os.environ.copy()
    if env:
        run_env.update(env)
    run_env.setdefault("PYTHONPATH", DEFAULT_PYTHONPATH)

    result = subprocess.run(
        cmd,
        cwd=str(FLINK_HOME),
        env=run_env,
        capture_output=True,
        text=True,
        check=False,
    )
    output = (result.stdout or "") + (result.stderr or "")
    if result.returncode != 0:
        raise RuntimeError(f"flink run failed (exit {result.returncode}):\n{output}")

    return parse_submitted_job_id(output), output
