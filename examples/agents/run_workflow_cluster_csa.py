#!/usr/bin/env python3
"""Cluster runner for a single agent on Cloudera CSA (Flink 1.20 on YARN).

The CSA counterpart of ``run_workflow_cluster.py``. The pipeline is deliberately
minimal — from_collection -> apply(<Agent>) -> print — so that any failure is
attributable to the deployment, not to the agent. Point the import below at whichever
agent module you are deploying; keep the pipeline shape as it is.

Note this filename is referenced in two places, so renaming it is not free:
``scripts/build_csa_bundle.sh`` copies it into the bundle by name, and
``scripts/submit_agent_csa.sh`` defaults ``ENTRY`` to it.

Differences from the Docker runner:

* No ``/opt/flink`` on sys.path. ``-pyfs agentcode.zip`` puts the shipped modules
  there; only this script's own directory needs adding, for a direct local run.
* No ``patch_flink_agents_version()``. That exists because the Docker image uses
  Flink's bundled ``pyflink.zip`` and has no ``apache-flink`` dist metadata for
  Flink Agents to read. On CSA the metadata is real — Cloudera pip-installs
  ``apache-flink`` (1.20.5 on CSA 1.18.0.0) into the node's Python 3.11, and
  ``scripts/build_csa_venv_gateway.sh`` builds a ``--system-site-packages`` venv that
  inherits it — so fabricating it would only risk disagreeing with it.
* ``patch_flink_agents_jar_loading()`` is still applied, and matters more here, not
  less: it forces common+thin jars through a single ``add_jars`` call so pemja
  resolves to one class identity. Flink Agents' own loader adds them one at a time,
  which splits pemja across classloaders and fails with a ClassCastException on
  ``pemja.core.object.PyObject`` (FLINK-39226).
"""

from __future__ import annotations

import os
import sys
from pathlib import Path


def _bootstrap() -> None:
    """Make the shipped modules importable and point the runtime at this env."""
    here = Path(__file__).resolve().parent
    for candidate in (here, here.parent, here.parent.parent):
        if (candidate / "ratatoskr").is_dir() and str(candidate) not in sys.path:
            sys.path.insert(0, str(candidate))
    if str(here) not in sys.path:
        sys.path.insert(0, str(here))

    # The Flink Agents jars sit inside this environment's site-packages under
    # flink_agents/lib/{common,flink-<minor>}/ — the flink-agents wheel itself bundles
    # all six per-minor dist jars, so installing the wheel stages them. Derive that
    # location from the installed package instead of requiring the caller to know it,
    # so the script works however it is invoked. Must happen before
    # flink_cluster_submit is imported — it resolves paths at import time.
    #
    # Only the CLIENT needs these: they reach the TaskManagers as YARN local resources
    # via pipeline.jars, which is why the archive can be built with STRIP_JARS=1.
    if not os.environ.get("RATATOSKR_SITE_PACKAGES"):
        try:
            import flink_agents

            site_packages = Path(flink_agents.__file__).resolve().parent.parent
            os.environ["RATATOSKR_SITE_PACKAGES"] = str(site_packages)
        except Exception:
            pass


def _preflight() -> None:
    """Fail loudly and early on the two misconfigurations that cost the most time."""
    from ratatoskr.runtime.flink_cluster_submit import (
        FLINK_HOME,
        SITE_PACKAGES,
        flink_agents_jar_uris,
        flink_major_version,
    )

    if not (FLINK_HOME / "lib").is_dir():
        raise SystemExit(
            f"FLINK_HOME does not look like a Flink install: {FLINK_HOME}\n"
            "Set it to the CSA parcel, e.g.\n"
            "  export FLINK_HOME=/opt/cloudera/parcels/FLINK/lib/flink\n"
            "The Flink version is read from its lib/flink-dist*.jar, and every\n"
            "version-derived jar path depends on it."
        )

    major = flink_major_version()
    jars = flink_agents_jar_uris(pipeline=False)
    if not jars:
        raise SystemExit(
            f"No Flink Agents jars for Flink {major} under {SITE_PACKAGES}.\n"
            f"Expected flink_agents/lib/common/ and flink_agents/lib/flink-{major}/.\n"
            "Either the bundle was built for a different Flink minor than the\n"
            "cluster runs, or RATATOSKR_SITE_PACKAGES points at the wrong env."
        )

    print(f"[preflight] FLINK_HOME     = {FLINK_HOME}")
    print(f"[preflight] Flink version  = {major}")
    print(f"[preflight] site-packages  = {SITE_PACKAGES}")
    for uri in jars:
        print(f"[preflight] agents jar     = {uri}")


def main() -> None:
    _bootstrap()
    _preflight()

    from ratatoskr.runtime.flink_agents_bootstrap import patch_flink_agents_jar_loading

    patch_flink_agents_jar_loading()

    from pyflink.datastream import StreamExecutionEnvironment

    from flink_agents.api.execution_environment import AgentsExecutionEnvironment

    from examples.agents.threshold_monitor import ThresholdMonitorAgent

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    agents_env = AgentsExecutionEnvironment.get_execution_environment(env)

    records = [{"key": str(i), "value": i * 5} for i in range(1, 4)]
    stream = env.from_collection(records)
    keyed = agents_env.from_datastream(
        input=stream,
        key_selector=lambda row: row["key"],
    )
    out = keyed.apply(ThresholdMonitorAgent()).to_datastream()
    out.print()
    agents_env.execute("Ratatoskr Threshold Monitor (CSA)")


if __name__ == "__main__":
    main()
