"""Motor tables: the axes each arm's board drives, and the Configure parameters each one takes.

A board does not keep its motor tables across a power cycle and cannot be asked for them, so the
host loads them at a cold start. Two more values per axis never reach a board: the Master
Controller keeps them in its own motor record and applies them itself. `home_position` is its
`phome`: where its Home All leaves X, the Multichannel's Y and the gripper axes (see
`features/homing.py`; the Z channels, the plunger and the Span-8 Y channels end elsewhere).
`hysteresis` is what it adds to an axis's targets; only the Span-8 Z channels have any, and it
differs channel by channel. They are data, recorded from what the Master Controller sent an
instrument (the `modules` section of its declared configuration, `driver/configuration.py`), not
something the driver works out. No instrument's tables ship with the driver: each instrument's
declared configuration is given by path, or by `$BIOMEK_I7_CONFIGURATION`.

Three layouts of Configure parameters were seen, each starting with the axis's limits:

  CiA 402 drive (X, Y, Z, D, grippers):  0x00 | 6 x u32 velocity/acceleration/jerk limits |
                                          s32 min position | s32 max position | ...
  SAGIAN drive (Span-8 Y1-Y8, Z1-Z8):    0xFF | the same limits | ...
  syringe pump (Span-8 D1-D8):           8 x u32 velocity/acceleration limits |
                                          s32 min position | s32 max position | u16 backlash
"""

import json
import os
import struct
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Union

from pylabrobot.beckman_coulter.biomek_i7.driver.protocol import Device

# Names the instrument's declared configuration (driver/configuration.py) when no path is given.
CONFIGURATION_ENV = "BIOMEK_I7_CONFIGURATION"


def declared_configuration_path(path: Optional[Union[str, Path]] = None) -> Path:
  """`path`, else the file `$BIOMEK_I7_CONFIGURATION` names.

  Raises:
    ValueError: with neither: no instrument's configuration ships with the driver.
  """
  path = path or os.environ.get(CONFIGURATION_ENV)
  if not path:
    raise ValueError(
      "no declared configuration: pass the path of the instrument's (the reverse-engineering "
      f"workspace's tools/import_bif.py writes one from its .bif) or set ${CONFIGURATION_ENV}"
    )
  return Path(path)


ModuleType = Literal["multichannel", "span8"]


@dataclass(frozen=True)
class AxisLimits:
  """An axis's limits, as the Master Controller's motor record has them (`vmin` .. `jmax`). A
  `max_jerk` of 0 means the axis takes no jerk (a trapezoidal profile: the Multichannel D and the
  gripper Z, and the syringe pumps)."""

  min_position: int
  max_position: int
  max_velocity: int
  max_acceleration: int
  max_jerk: int
  min_velocity: int = 1
  min_acceleration: int = 1
  min_jerk: int = 0


@dataclass(frozen=True)
class PumpLimits:
  """A syringe pump's own parameters (the Master Controller's `<pump-config>`), velocities in
  counts/s and `backlash` in counts."""

  min_start_velocity: int
  max_start_velocity: int
  min_cutoff_velocity: int
  max_cutoff_velocity: int
  backlash: int


@dataclass(frozen=True)
class Axis:
  """One axis an arm's board drives, in the board's counts."""

  name: str
  device: int
  index: int
  configure: bytes
  home_position: int = 0
  hysteresis: int = 0

  @property
  def is_pump(self) -> bool:
    return self.device == Device.PUMP

  @property
  def drive(self) -> Literal["cia402", "sagian", "pump"]:
    """The kind of drive, read off the Configure layout. A CiA 402 drive reports a fault whenever
    its motor power is off (interlock tripped); a SAGIAN drive or pump does not."""
    if self.is_pump:
      return "pump"
    return "sagian" if self.configure[0] == 0xFF else "cia402"

  @property
  def limits(self) -> AxisLimits:
    """The limits at the head of the axis's Configure parameters."""
    if self.is_pump:
      values = struct.unpack_from("<8I2i", self.configure)
      return AxisLimits(values[8], values[9], values[3], values[7], 0, values[2], values[6], 0)
    values = struct.unpack_from("<6I2i", self.configure, 1)
    return AxisLimits(values[6], values[7], values[1], values[3], values[5], *values[0:5:2])

  @property
  def pump_limits(self) -> PumpLimits:
    """A syringe pump's start and cutoff velocity limits and backlash, from its Configure
    parameters (min/max start velocity, min/max velocity, min/max cutoff velocity, min/max
    acceleration, min/max position, backlash).

    Raises:
      ValueError: if the axis is not a pump.
    """
    if not self.is_pump:
      raise ValueError(f"{self.name} is not a syringe pump")
    values = struct.unpack_from("<8I2iH", self.configure)
    return PumpLimits(values[0], values[1], values[4], values[5], values[10])

  def check_position(self, position: int) -> None:
    """Raise `ValueError` for a target outside the axis's position limits."""
    limits = self.limits
    if not limits.min_position <= position <= limits.max_position:
      raise ValueError(
        f"{self.name} position {position} is outside ({limits.min_position}, {limits.max_position})"
      )


@dataclass(frozen=True)
class AxisMove:
  """One axis's next move, in counts. Speeds left as None are the axis's maximum; `force` is the
  force limit x1000 (grip and tip loading), 0 for none; `sensor` and `trigger` stop the move
  where that sensor fires (0, 0 for none)."""

  axis: str
  position: int
  velocity: Optional[int] = None
  acceleration: Optional[int] = None
  jerk: Optional[int] = None
  force: int = 0
  sensor: int = 0
  trigger: int = 0


class MotorTables:
  """The axes of every arm type in one recording, in the order they are configured."""

  def __init__(self, modules: Dict[str, List[Axis]]):
    self.modules = modules

  @classmethod
  def load(cls, path: Optional[Union[str, Path]] = None) -> "MotorTables":
    """The `modules` section of a recording: a declared configuration, or a motor recording. The
    instrument's declared configuration when `path` is None (`declared_configuration_path`)."""
    with open(declared_configuration_path(path), encoding="utf-8") as fh:
      data = json.load(fh)
    return cls.from_dict(data["modules"])

  @classmethod
  def from_dict(cls, modules: Dict[str, Any]) -> "MotorTables":
    """From a recording's `modules` section: per arm type, its axes as recorded."""
    return cls(
      {
        module: [
          Axis(
            a["name"],
            a["device"],
            a["index"],
            bytes.fromhex(a["configure"]),
            a.get("home_position", 0),
            a.get("hysteresis", 0),
          )
          for a in axes
        ]
        for module, axes in modules.items()
      }
    )

  def axes(self, module: str) -> List[Axis]:
    """The axes of one arm type.

    Raises:
      ValueError: if the recording has no tables for that type.
    """
    if module not in self.modules:
      raise ValueError(f"no motor tables recorded for a {module} arm; have {sorted(self.modules)}")
    return self.modules[module]
