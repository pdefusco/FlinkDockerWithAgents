"""Bootstrap Flink Agents on the Flink Docker image (bundled PyFlink, no apache-flink pip)."""

from __future__ import annotations


def patch_flink_agents_version() -> None:
    """Register the cluster Flink version when apache-flink pip metadata is absent."""
    try:
        from importlib.metadata import version as pkg_version

        pkg_version("apache-flink")
    except Exception:
        try:
            from flink_agents.api import version_compatibility
            from ratatoskr.runtime.flink_cluster_submit import flink_major_version

            major = flink_major_version()
            version_compatibility.flink_version_manager._flink_version = f"{major}.0"
            version_compatibility.flink_version_manager._initialized = True
        except Exception:
            pass

    patch_flink_agents_jar_loading()


def patch_flink_agents_config_loading() -> None:
    """Stop Flink Agents crashing on Flink's *legacy* ``flink-conf.yaml`` format.

    ``RemoteExecutionEnvironment.__load_config_from_flink_conf_dir`` reads
    ``$FLINK_CONF_DIR/flink-conf.yaml`` and hands it to ``yaml.safe_load``. That is an
    upstream bug: Flink has **two** config formats, and only the newer ``config.yaml`` is
    real YAML. The legacy ``flink-conf.yaml`` is a flat ``key: value`` file parsed by a
    hand-rolled line splitter in ``GlobalConfiguration``, and it is *not* required to be
    valid YAML. Cloudera CSA ships the legacy format, so on CSA this raises::

        yaml.scanner.ScannerError: while scanning for the next token
        found character '%' that cannot start any token
          in "/etc/flink/conf/flink-conf.yaml", line 49, column 40

    Line 49 is ``yarn.container-start-command-template``, whose value contains Flink's own
    ``%java% %jvmmem% %jvmopts%`` placeholders. A bare ``%`` cannot start a YAML token.

    The insult is that the parse is pointless here: ``load_from_file`` only keeps
    ``raw_config.get("agent", {})``, and CSA's config has no ``agent`` keys at all — so
    the correct result is an empty dict, and the crash is pure collateral damage.

    Note this bug is *reachable only because* ``FLINK_CONF_DIR`` is now set. The loader
    returns early when it is unset, which is why earlier submissions never got here.
    Unsetting it is not an option — the parcel's shell scripts, ``--add-opens``, and every
    YARN/HA/security default depend on it (see ``scripts/submit_agent_csa.sh``).

    The patch parses both formats and keeps the ``agent.*`` keys either way. It only
    changes behaviour when ``yaml.safe_load`` *fails*, so the Docker path — whose
    ``/opt/flink/conf`` config does parse as YAML — is byte-for-byte unchanged.
    """
    try:
        from flink_agents.plan import configuration as cfg
    except ImportError:
        return
    if getattr(cfg, "_RATATOSKR_CONFIG_PATCH", False):
        return

    # ``Configuration`` is only the ABC; the implementation lives on
    # ``AgentConfiguration(BaseModel, Configuration)``. Check ``vars(cls)`` rather than
    # ``hasattr`` so an inherited abstract stub is not mistaken for the real method.
    target = None
    for name in ("AgentConfiguration", "Configuration"):
        cls = getattr(cfg, name, None)
        if cls is not None and "load_from_file" in vars(cls):
            target = cls
            break
    if target is None:
        return

    _original_load = target.load_from_file

    def _patched_load_from_file(self, config_path=None):
        try:
            return _original_load(self, config_path)
        except Exception:
            pass
        if not config_path:
            return None

        # Legacy fallback: flat "key: value" lines, splitting on the FIRST colon only,
        # because values routinely contain colons (URIs, java opts, ZK quorums).
        from pathlib import Path

        agent_conf: dict[str, str] = {}
        try:
            for raw_line in Path(config_path).read_text().splitlines():
                line = raw_line.strip()
                if not line or line.startswith("#") or ":" not in line:
                    continue
                key, _, value = line.partition(":")
                key = key.strip()
                if key.startswith("agent."):
                    agent_conf[key[len("agent.") :]] = value.strip()
        except OSError:
            return None

        self.conf_data.update(agent_conf)
        return None

    target.load_from_file = _patched_load_from_file
    cfg._RATATOSKR_CONFIG_PATCH = True


def patch_flink_agents_jar_loading() -> None:
    """Skip duplicate per-jar ``add_jars`` calls from Flink Agents (Pemja classloaders)."""
    # Must be installed before create_instance() below, which is what triggers the
    # FLINK_CONF_DIR read. No-op wherever the config is valid YAML.
    patch_flink_agents_config_loading()

    try:
        from flink_agents.api import execution_environment as ee
    except ImportError:
        return
    if getattr(ee, "_RATATOSKR_JAR_PATCH", False):
        return

    import importlib

    _original_get = ee.AgentsExecutionEnvironment.get_execution_environment

    def _patched_get_execution_environment(env=None, t_env=None, **kwargs):
        if env is None:
            return _original_get(env=env, t_env=t_env, **kwargs)

        try:
            from flink_agents.api import version_compatibility
            from ratatoskr.runtime.flink_cluster_submit import (
                attach_flink_agents_jars,
                flink_major_version,
            )

            if not version_compatibility.flink_version_manager._initialized:
                major = flink_major_version()
                version_compatibility.flink_version_manager._flink_version = f"{major}.0"
                version_compatibility.flink_version_manager._initialized = True
            major_version = version_compatibility.flink_version_manager.major_version
        except Exception as exc:
            raise ModuleNotFoundError("Apache Flink is not installed.") from exc

        if not major_version:
            raise ModuleNotFoundError("Apache Flink is not installed.")

        attach_flink_agents_jars(env)

        return importlib.import_module(
            "flink_agents.runtime.remote_execution_environment"
        ).create_instance(env=env, t_env=t_env, **kwargs)

    ee.AgentsExecutionEnvironment.get_execution_environment = staticmethod(
        _patched_get_execution_environment
    )
    ee._RATATOSKR_JAR_PATCH = True
