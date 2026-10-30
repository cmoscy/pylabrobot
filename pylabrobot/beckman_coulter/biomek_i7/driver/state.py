"""What the driver must remember about an instrument between sessions, because the boards cannot
tell it: which tips each head carries, and counters Biomek keeps for maintenance.

The Multichannel head has no tip sensor (the Span-8's channels do: `GT`), so whether it holds
tips is known only from what was last done with it. Biomek keeps this in its instrument file
(`RT_Tips`, `LastTips`, per channel `RT_Tip1`..`RT_Tip8`) and its pump cycle counts
(`DAxisCycles`), and rewrites them as it runs.

This is not configuration (`driver/configuration.py`), which holds facts that change only with
calibration or hardware. State changes with every operation. The driver keeps it in memory, and
in a JSON file when given one (`BiomekI7Driver(state_json=...)`).

A carried tip is remembered as PyLabRobot defines it (`Tip.serialize`), so any tip, Biomek's or
another, is known again after a restart: its length places the head.
"""

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from pylabrobot.beckman_coulter.biomek_i7.driver.configuration import Side
from pylabrobot.resources.resource import Resource
from pylabrobot.resources.tip import Tip


def tip_key(tip: Tip) -> str:
  """What `PodState.tips` names a tip by: its model, else its name."""
  return tip.model or tip.name


@dataclass
class PodState:
  """One pod's remembered state. `tips` names the tip on each channel (one entry per mandrel of
  the Multichannel head, eight for a Span-8; `tip_key`), None for no tip, and `tip_types` holds each
  named tip's definition (`Tip.serialize`). `d_axis_cycles` counts plunger or pump strokes per
  axis, as Biomek's maintenance counters do."""

  tips: List[Optional[str]] = field(default_factory=list)
  tip_types: Dict[str, Dict[str, Any]] = field(default_factory=dict)
  d_axis_cycles: Dict[str, int] = field(default_factory=dict)

  def has_tips(self) -> bool:
    return any(t is not None for t in self.tips)

  def set_tip(self, index: int, tip: Optional[Tip]) -> None:
    """Channel `index` carries `tip` (None: no tip). Definitions no channel carries are dropped."""
    if len(self.tips) <= index:
      self.tips.extend([None] * (index + 1 - len(self.tips)))
    if tip is None:
      self.tips[index] = None
    else:
      key = tip_key(tip)
      self.tips[index] = key
      self.tip_types[key] = tip.serialize()
    carried = {t for t in self.tips if t is not None}
    self.tip_types = {k: v for k, v in self.tip_types.items() if k in carried}

  def tip(self, index: int) -> Optional[Tip]:
    """The tip channel `index` carries, rebuilt from its definition; None for no tip, or one whose
    definition was not kept."""
    key = self.tips[index] if index < len(self.tips) else None
    record = self.tip_types.get(key) if key is not None else None
    if record is None:
      return None
    tip = Resource.deserialize(record)
    assert isinstance(tip, Tip)
    return tip


@dataclass
class SessionState:
  pods: Dict[Side, PodState] = field(default_factory=dict)

  def pod(self, side: Side) -> PodState:
    """This side's state, created empty on first use."""
    return self.pods.setdefault(side, PodState())

  @classmethod
  def load(cls, path: Union[str, Path]) -> "SessionState":
    """Read a saved state. A path that does not exist yet gives an empty state."""
    if not Path(path).exists():
      return cls()
    with open(path, encoding="utf-8") as fh:
      data = json.load(fh)
    state = cls({side: PodState(**p) for side, p in data.get("pods", {}).items()})
    for pod in state.pods.values():
      _define_biomek_tips(pod)
    return state

  def save(self, path: Union[str, Path]) -> None:
    data = {"pods": {side: asdict(p) for side, p in self.pods.items()}}
    Path(path).write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")


def _define_biomek_tips(pod: PodState) -> None:
  """A state saved before tips were kept with their definitions names Biomek tip classes only
  (`biomek_<class>`): define those from the class."""
  from pylabrobot.beckman_coulter.biomek_i7.resource_model.labware import biomek_tip

  for key in pod.tips:
    if key is not None and key not in pod.tip_types and key.startswith("biomek_"):
      pod.tip_types[key] = biomek_tip(key[len("biomek_") :], key).serialize()
