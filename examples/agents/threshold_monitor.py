"""Deterministic threshold monitor — flattened from Designer def_d92f621f70aa.

Deliberately narrow: stdlib plus flink_agents.api.* only, so the same module loads
under Flink 1.20.5 on CSA and Flink 2.1.3 in the local containers.
"""

from __future__ import annotations

from flink_agents.api.agents.agent import Agent
from flink_agents.api.decorators import action, tool
from flink_agents.api.events.event import Event, InputEvent, OutputEvent
from flink_agents.api.runner_context import RunnerContext

_INPUT_EVENT = InputEvent.EVENT_TYPE

SCALE = 3
THRESHOLD = 20


def _int_from_input(event: Event, *, field: str = "value") -> int:
    payload = InputEvent.from_event(event).input
    if isinstance(payload, dict):
        raw = payload.get(field, 0)
    else:
        raw = getattr(payload, field, payload)
    return int(raw)


class ThresholdMonitorAgent(Agent):
    """Scales each reading by a fixed factor and flags readings over THRESHOLD."""

    @tool
    @staticmethod
    def scale(value: int) -> int:
        """Return the reading multiplied by the fixed scale factor."""
        return value * SCALE

    @action(_INPUT_EVENT)
    @staticmethod
    def process(event: Event, ctx: RunnerContext) -> None:
        reading = _int_from_input(event)
        scaled = ThresholdMonitorAgent.scale(reading)
        status = "ALERT" if scaled > THRESHOLD else "OK"
        ctx.send_event(
            OutputEvent(
                output={
                    "reading": reading,
                    "scaled": scaled,
                    "threshold": THRESHOLD,
                    "status": status,
                    "agent": "threshold_monitor",
                }
            )
        )
