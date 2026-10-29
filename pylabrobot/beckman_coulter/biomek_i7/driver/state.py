"""What the driver must remember about an instrument between sessions, because the boards cannot
tell it: which tips each head carries, and counters Biomek keeps for maintenance.

The Multichannel head has no tip sensor (the Span-8's channels do: `GT`), so whether it holds
tips is known only from what was last done with it. Biomek keeps this in its instrument file
(`RT_Tips`, `LastTips`, per channel `RT_Tip1`..`RT_Tip8`) and its pump cycle counts
(`DAxisCycles`), and rewrites them as it runs.

This is not configuration (`driver/configuration.py`), which holds facts that change only with
calibration or hardware. State changes with every operation. The driver keeps it in memory, and
in a JSON file when given one (`BiomekI7Driver(state_json=...)`).
"""

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Union

from pylabrobot.beckman_coulter.biomek_i7.driver.configuration import Side


@dataclass
class PodState:
  """One pod's remembered state. `tips` is the tip type on each channel (one entry for the
  Multichannel head, eight for a Span-8), None for no tip. `d_axis_cycles` counts plunger or pump
  strokes per axis, as Biomek's maintenance counters do."""

  tips: List[Optional[str]] = field(default_factory=list)
  d_axis_cycles: Dict[str, int] = field(default_factory=dict)

  def has_tips(self) -> bool:
    return any(t is not None for t in self.tips)


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
    return cls({side: PodState(**p) for side, p in data.get("pods", {}).items()})

  def save(self, path: Union[str, Path]) -> None:
    data = {"pods": {side: asdict(p) for side, p in self.pods.items()}}
    Path(path).write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
