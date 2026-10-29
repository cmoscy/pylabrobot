"""What the driver knows about an instrument: facts only (STAR design.md P3).

Two kinds:

- **What the boards answer** about themselves at setup (`ModuleConfiguration`, `BoardInfo`).
- **What they cannot answer**, declared per instrument (`DeviceConfiguration`):
  - the motor tables loaded into the boards after every power-up;
  - per pod, the counts conversions, travel limits, geometry and settings that Biomek keeps on
    the host side.

  It is read from one JSON file per instrument (written by the reverse-engineering workspace's
  `tools/import_bif.py` and `tools/extract_motor_tables.py`), given by path or by
  `$BIOMEK_I7_CONFIGURATION`; none ships with the driver. It is saved back with `save`, and
  records where each part came from.
"""

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple, Union

from pylabrobot.beckman_coulter.biomek_i7.driver.motors import (
  MotorTables,
  declared_configuration_path,
)
from pylabrobot.beckman_coulter.biomek_i7.driver.protocol import HardwareVersion, ModuleId


# The firmware this driver has been run against (H1 and H2, 2026-10-24 and 2026-10-27): every
# motion board and module the same image, and the other boards' application versions (chassis
# 0.2.4, SAGIAN Motion 0.3.14, LLS 0.7.8, syringe pump 5.0.9). The Master Controller flashes the
# boards when it brings other firmware, which would change what the boards answer.
TESTED_FIRMWARE_IMAGES = {(0x6D2DDB78, 266436)}
TESTED_APPLICATION_VERSIONS = {"0.2.4", "0.3.14", "0.7.8", "5.0.9"}


@dataclass
class BoardInfo:
  """One board inside a module, as the module reports it."""

  index: int
  hardware: HardwareVersion
  serial_number: Optional[str] = None
  application_version: Optional[str] = None
  application_crc: Optional[int] = None
  application_length: Optional[int] = None


@dataclass
class ModuleConfiguration:
  """One ACE module (the chassis, or an arm): its identity, its boards, and its inputs."""

  module_id: ModuleId
  usb_serial_number: Optional[str]
  serial_number: str
  hardware: HardwareVersion
  application_crc: int
  application_length: int
  device_counts: Dict[int, int]
  boards: List[BoardInfo] = field(default_factory=list)
  analog_inputs: Dict[int, str] = field(default_factory=dict)
  analog_units: Dict[int, str] = field(default_factory=dict)
  digital_inputs: Dict[int, str] = field(default_factory=dict)
  digital_outputs: Dict[int, str] = field(default_factory=dict)

  def untested_firmware(self) -> List[str]:
    """What this module runs that the driver has not been run against: one line per image or
    version, empty when everything is tested."""
    found = []
    if (self.application_crc, self.application_length) not in TESTED_FIRMWARE_IMAGES:
      found.append(
        f"module image CRC {self.application_crc:#010x}, {self.application_length} bytes"
      )
    for b in self.boards:
      image = (b.application_crc, b.application_length)
      if b.application_crc is not None and image not in TESTED_FIRMWARE_IMAGES:
        found.append(
          f"board {b.index} image CRC {b.application_crc:#010x}, {b.application_length} bytes"
        )
      if (
        b.application_version is not None
        and b.application_version not in TESTED_APPLICATION_VERSIONS
      ):
        found.append(f"board {b.index} application {b.application_version}")
    return found


# -- declared per instrument -------------------------------------------------------------------------

Side = Literal["left", "right"]


@dataclass(frozen=True)
class AxisConversion:
  """How Biomek turns a value into counts for one axis or quantity: `counts = (value + offset) x
  const`, rounded. `unit` is what `value` is in: Biomek's own units, `cm`, `deg` or `uL`.

  Read from the instrument file's `CmdGen/Conversion` (`<axis>Const`, `<axis>Offset`). Checked on
  this instrument against positions it logged:
  - the Multichannel's park GR, 90 deg, is -175,443 counts;
  - its rest Z, 39.763 cm, is -256 counts;
  - the Span-8 Z park, 39.1079 cm on Z1, is 9,029 counts.
  """

  const: float
  offset: float
  unit: str

  def to_counts(self, value: float) -> int:
    return round((value + self.offset) * self.const)

  def to_value(self, counts: int) -> float:
    return counts / self.const - self.offset


@dataclass(frozen=True)
class PodGeometry:
  """A pod's dimensions as Biomek's collision model uses them, in cm.

  From the instrument file's per-device pod entry. That entry, not the pod type's defaults under
  `HardwareSetupInfo/PodTypes`, is what reproduces the travel Biomek logged:
  - BridgeXSpan is 39.0154 here, 12.7 in the type defaults;
  - the bridges touch at left X + |right X| = 250,807 counts, and the most Biomek ever commanded
    was 250,678.

  The optional fields are absent on a pod type that has no such part.
  """

  bridge_x_span: float
  bridge_to_head_x_offset: float
  mandrel1_x_offset: float
  mandrel1_y_offset: float
  mandrel_x_spacing: float
  mandrel_y_spacing: float
  avoidance_margin: float
  pod_x_span: float
  pod_y_span: float
  gripper_x_offset: float
  gripper_y_offset: float
  gripper_z_offset: float
  frame_safety_left: float
  frame_safety_right: float
  additional_safe_height: float
  additional_safe_height_gripper: float
  mandrels_x: Optional[int] = None
  mandrels_y: Optional[int] = None
  sub_mandrel_safe_height: Optional[float] = None


@dataclass(frozen=True)
class Obstacle:
  """A fixed obstacle on the deck, as Biomek keeps it per pod (`Obstacles` in the instrument
  file): a box in Biomek's frame (cm; Y towards the front, Z up from the deck surface),
  a bound that is None open on that side, widened by `buffer`. Walls (the shields, the towers, the
  light curtain) have no Z bounds; the pump bank is a floor up to `z2`; the I-beam a ceiling from
  `z1`."""

  name: str
  buffer: float = 0.0
  x1: Optional[float] = None
  x2: Optional[float] = None
  y1: Optional[float] = None
  y2: Optional[float] = None
  z1: Optional[float] = None
  z2: Optional[float] = None


@dataclass
class PodConfiguration:
  """One pod's facts the boards do not hold.

  - `conversion`: per axis or quantity, keyed by Biomek's own names (`X`, `Y`, `Z`, `D`, `GY`,
    `GG`, `GZ`, `GR`; on a Span-8 also `S` for the span, `Z1`..`Z8`, `D1`..`D8`).
  - `limits`: each axis's travel in those units, `(lowest, highest)`.
  - `park`: Biomek's park location.
  - `settings`: every other scalar Biomek keeps for the pod, and the Span-8's per-probe lists
    (`LoadingOffsets`, `BrokenTips`) (tip loading, safe heights, speeds,
    gripper squeeze) under its own name, for the features that will use them.
  - `gripper_model`: the gripper's dimensions, as Biomek nests them.
  """

  component: str
  module_type: str
  side: Side
  conversion: Dict[str, AxisConversion]
  limits: Dict[str, Tuple[float, float]]
  geometry: PodGeometry
  park: Dict[str, float] = field(default_factory=dict)
  settings: Dict[str, Any] = field(default_factory=dict)
  gripper_model: Dict[str, Any] = field(default_factory=dict)
  obstacles: List[Obstacle] = field(default_factory=list)
  source: str = ""

  def to_counts(self, axis: str, value: float) -> int:
    """A value in Biomek's units for `axis` (see `conversion`) as counts.

    Raises:
      ValueError: for an axis with no conversion.
    """
    return self._conversion(axis).to_counts(value)

  def to_value(self, axis: str, counts: int) -> float:
    """Counts on `axis` as a value in Biomek's units for it."""
    return self._conversion(axis).to_value(counts)

  def _conversion(self, axis: str) -> AxisConversion:
    if axis not in self.conversion:
      raise ValueError(
        f"{self.component} has no conversion for {axis!r}; has {sorted(self.conversion)}"
      )
    return self.conversion[axis]


@dataclass(frozen=True)
class DeckPosition:
  """A deck position as Biomek calibrated it, in Biomek's frame (cm; Y towards the front, Z up
  from the deck surface): a corner per pod, by Biomek's pod number, and the position's spans.

  Each pod moves to a position relative to its own corner: corner n is the one that shifts when
  pod n's deck framing changes (`ShiftDeck<n>` applied to `X<n>`/`Y<n>`/`Z<n>`). The logged moves
  confirm it: the Multichannel's (Pod1) heights
  track Z1 and the Span-8's (Pod2) Z2 (`tools/validate_frames.py`). `framed[n]` says how corner n
  was obtained: `Position` (framed at this position) or `Deck` (from the deck's framing).
  """

  corners: Dict[int, Tuple[float, float, float]]
  x_span: float
  y_span: float
  framed: Dict[int, str] = field(default_factory=dict)

  def corner(self, pod_number: int) -> Tuple[float, float, float]:
    """Raises:
    ValueError: for a pod with no corner here.
    """
    if pod_number not in self.corners:
      raise ValueError(f"no corner for pod {pod_number}; have {sorted(self.corners)}")
    return self.corners[pod_number]


@dataclass
class DeviceConfiguration:
  """Everything declared about one instrument: its motor tables and its pods."""

  chassis_serial_number: str
  instrument_serial_number: Optional[str]
  modules: Dict[str, Any]
  pods: Dict[Side, PodConfiguration]
  sources: Dict[str, str] = field(default_factory=dict)
  # The default deck's positions by Biomek's name (`P1`, `TL1`), as calibrated; empty if not
  # imported.
  deck_positions: Dict[str, DeckPosition] = field(default_factory=dict)

  @property
  def motor_tables(self) -> MotorTables:
    return MotorTables.from_dict(self.modules)

  def pod(self, side: Side) -> PodConfiguration:
    """Raises:
    ValueError: if nothing is declared for that side.
    """
    if side not in self.pods:
      raise ValueError(f"no pod declared on the {side}; declared: {sorted(self.pods)}")
    return self.pods[side]

  @classmethod
  def load(cls, path: Optional[Union[str, Path]] = None) -> "DeviceConfiguration":
    """An instrument's declared configuration, from `path`, else the file
    `$BIOMEK_I7_CONFIGURATION` names.

    Raises:
      ValueError: with neither.
    """
    with open(declared_configuration_path(path), encoding="utf-8") as fh:
      data = json.load(fh)
    return cls.from_dict(data)

  def save(self, path: Union[str, Path]) -> None:
    Path(path).write_text(json.dumps(self.to_dict(), indent=2) + "\n", encoding="utf-8")

  @classmethod
  def from_dict(cls, data: Dict[str, Any]) -> "DeviceConfiguration":
    pods: Dict[Side, PodConfiguration] = {}
    for side, p in data.get("pods", {}).items():
      pods[side] = PodConfiguration(
        component=p["component"],
        module_type=p["module_type"],
        side=side,
        conversion={k: AxisConversion(**v) for k, v in p["conversion"].items()},
        limits={k: (v[0], v[1]) for k, v in p["limits"].items()},
        geometry=PodGeometry(**p["geometry"]),
        park=dict(p.get("park", {})),
        settings=dict(p.get("settings", {})),
        gripper_model=dict(p.get("gripper_model", {})),
        obstacles=[Obstacle(**o) for o in p.get("obstacles", [])],
        source=p.get("source", ""),
      )
    positions = {
      name: DeckPosition(
        corners={int(n): (c[0], c[1], c[2]) for n, c in p["corners"].items()},
        x_span=p["x_span"],
        y_span=p["y_span"],
        framed={int(n): v for n, v in p.get("framed", {}).items()},
      )
      for name, p in data.get("deck_positions", {}).items()
    }
    return cls(
      chassis_serial_number=data["chassis_serial_number"],
      instrument_serial_number=data.get("instrument_serial_number"),
      modules=data["modules"],
      pods=pods,
      sources=dict(data.get("sources", {})),
      deck_positions=positions,
    )

  def to_dict(self) -> Dict[str, Any]:
    return {
      "chassis_serial_number": self.chassis_serial_number,
      "instrument_serial_number": self.instrument_serial_number,
      "sources": self.sources,
      "pods": {
        side: {
          "component": p.component,
          "module_type": p.module_type,
          "source": p.source,
          "conversion": {k: asdict(c) for k, c in p.conversion.items()},
          "limits": {k: list(v) for k, v in p.limits.items()},
          "geometry": {k: v for k, v in asdict(p.geometry).items() if v is not None},
          "park": p.park,
          "settings": p.settings,
          "gripper_model": p.gripper_model,
          "obstacles": [{k: v for k, v in asdict(o).items() if v is not None} for o in p.obstacles],
        }
        for side, p in self.pods.items()
      },
      "deck_positions": {
        name: {
          "corners": {str(n): list(c) for n, c in p.corners.items()},
          "x_span": p.x_span,
          "y_span": p.y_span,
          "framed": {str(n): v for n, v in p.framed.items()},
        }
        for name, p in self.deck_positions.items()
      },
      "modules": self.modules,
    }
