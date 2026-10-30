"""Biomek's labware, as PyLabRobot resources, built from Biomek's own labware and tip classes.

`biomek_labware.json` holds every class captured from Biomek's project database (written by the
reverse-engineering workspace's `tools/import_labware.py`), under Biomek's names and in its
centimetres. This module builds resources from it the way the hand-transcribed definitions in
`pylabrobot.resources.beckman_coulter` are built (`labware_tests.py` checks they agree):

- a `TipBox` is a `StandingTipRack`: tip spots on the top face (`height`), holes of `wellradius`,
  A1's centre `well1xoffset` / `well1yoffset` from the left and back edges, filled with tips made
  from the box's tip class (`tipheight`, `capacity`, `maxtipradius`, `spanseatingdepth`);
- a `TiterPlate` is a `Plate`: wells `welldepth` deep, as wide as their top segment (`topx`,
  `topy`; `wellradius` when there are none), their bottom and cross-section from
  `bottomsectionshape` / `wellstyle`, and height <-> volume from the well's segments
  (`well_profile`, which also gives the wall's distance for a touch-off);
- a `Reservoir` is a `Plate` whose wells are its sections (`x1..x2`, `y1..y2` from the left and
  back edges, `depth`).

Lids, tube racks and reservations are not built yet.
"""

import functools
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from pylabrobot.resources.coordinate import Coordinate
from pylabrobot.resources.plate import Plate
from pylabrobot.resources.resource import Resource
from pylabrobot.resources.tip import Tip
from pylabrobot.resources.tip_rack import StandingTipRack, TipSpot
from pylabrobot.resources.utils import create_ordered_items_2d
from pylabrobot.resources.well import CrossSectionType, Well, WellBottomType
from pylabrobot.utils.interpolation import interpolate_1d

from .units import MM_PER_CM, cm

DATA = Path(__file__).with_name("biomek_labware.json")

# Biomek's bottom shapes, as PyLabRobot names them.
BOTTOM_TYPES = {
  "Flat": WellBottomType.FLAT,
  "V": WellBottomType.V,
  "Cone": WellBottomType.V,
  "Round": WellBottomType.U,
  "U": WellBottomType.U,
}
UL_PER_CM3 = 1000.0


@functools.lru_cache(maxsize=1)
def _classes() -> Dict[str, Dict[str, Any]]:
  with open(DATA, encoding="utf-8") as fh:
    return json.load(fh)  # type: ignore[no-any-return]


def biomek_labware_classes() -> List[str]:
  """Every labware class this module knows, by Biomek's name."""
  return sorted(_classes()["labware"])


def biomek_tip(class_name: str, name: str) -> Tip:
  """A tip of Biomek tip class `class_name` (`T1070`, `T230`, ...)."""
  tips = _classes()["tips"]
  if class_name not in tips:
    raise ValueError(f"no Biomek tip class {class_name!r}")
  t = tips[class_name]
  return Tip(
    name=name,
    model=f"biomek_{class_name}",
    diameter=2 * cm(t["maxtipradius"]),
    size_z=cm(t["tipheight"]),
    has_filter=bool(t.get("filtered", False)),
    maximal_volume=float(t["capacity"]),
    nominal_volume=float(t["capacity"]),
    fitting_depth=cm(t.get("spanseatingdepth", 0.0)),
  )


def tip_cone(tip: Tip) -> Optional[Tuple[float, float, float]]:
  """A Biomek tip's outside, cm: (radius at its end, widest radius, length over which it widens),
  from its class (`mintipradius`, `maxtipradius`, `coniclength`); None for a tip of no Biomek
  class."""
  model = tip.model or ""
  t = _classes()["tips"].get(model[len("biomek_") :]) if model.startswith("biomek_") else None
  if t is None:
    return None
  return float(t["mintipradius"]), float(t["maxtipradius"]), float(t.get("coniclength", 0.0))


def biomek_touch_distance(
  container: Resource, tip: Tip, above_bottom: float, theta: float = 90.0
) -> Optional[float]:
  """Biomek's touch-off distance, mm, as its logged touch-offs move, for `container` built
  from a Biomek class and a `tip` of one, with the tip's end `above_bottom` mm over the cavity
  bottom: the wall's distance at that height in direction (360 - theta) degrees, less the tip's
  radius where it crosses the rim (a cone from `mintipradius` at its end to `maxtipradius` over
  `coniclength`); 0 when that leaves no room. None for labware or tips of no Biomek class. Pass it as
  `TipTouch.distance`."""
  profile = well_profile(container)
  cone = tip_cone(tip)
  if profile is None or cone is None:
    return None
  z = above_bottom / MM_PER_CM
  wall = profile.wall_distance(z, math.radians(360.0 - theta))
  low, high, length = cone
  d = profile.depth - z  # from the rim down to the tip's end
  radius = high if length <= 0 else low + (high - low) * min(max(d, 0.0) / length, 1.0)
  return max(wall - radius, 0.0) * MM_PER_CM


def biomek_labware(class_name: str, name: str, with_tips: bool = True):  # type: ignore[no-untyped-def]
  """A resource of Biomek labware class `class_name` (`BC1070`, `BCFlat96`, `Reservoir`, ...).

  Raises:
    ValueError: for a class this module does not know.
    NotImplementedError: for a kind of labware it does not build yet (lids, tube racks).
  """
  classes = _classes()["labware"]
  if class_name not in classes:
    raise ValueError(f"no Biomek labware class {class_name!r}")
  c = classes[class_name]
  kind = c.get("type")
  if kind == "TipBox":
    return _tip_box(class_name, c, name, with_tips)
  if kind == "TiterPlate":
    return _plate(class_name, c, name)
  if kind == "Reservoir":
    return _reservoir(class_name, c, name)
  raise NotImplementedError(f"{class_name} is a {kind}; not built yet")


def _tip_box(class_name: str, c: Dict[str, Any], name: str, with_tips: bool) -> StandingTipRack:
  size_x, size_y, size_z = cm(c["xspan"]), cm(c["yspan"]), cm(c["height"])
  pitch_x, pitch_y = cm(c["wellxspacing"]), cm(c["wellyspacing"])
  hole = 2 * cm(c["wellradius"])
  rows = c["wellsy"]
  tip_class = c["tip_class"]
  return StandingTipRack(
    name=name,
    size_x=size_x,
    size_y=size_y,
    size_z=size_z,
    model=f"biomek_{class_name}",
    ordered_items=create_ordered_items_2d(
      TipSpot,
      num_items_x=c["wellsx"],
      num_items_y=rows,
      dx=cm(c["well1xoffset"]) - hole / 2,
      # The front row's hole, from the front edge: A1 is in the back row.
      dy=size_y - cm(c["well1yoffset"]) - (rows - 1) * pitch_y - hole / 2,
      dz=size_z,
      item_dx=pitch_x,
      item_dy=pitch_y,
      size_x=hole,
      size_y=hole,
      make_tip=lambda tip_name: biomek_tip(tip_class, tip_name),
      name_prefix=name,
    ),
    with_tips=with_tips,
  )


def _plate(class_name: str, c: Dict[str, Any], name: str) -> Plate:
  size_x, size_y, size_z = cm(c["xspan"]), cm(c["yspan"]), cm(c["height"])
  segments = c.get("segments") or []
  top = segments[-1] if segments else {}
  well_x = cm(top.get("topx", 2 * c["wellradius"]))
  well_y = cm(top.get("topy", 2 * c["wellradius"]))
  depth = cm(c["welldepth"])
  rows = c["wellsy"]
  pitch_x, pitch_y = cm(c["wellxspacing"]), cm(c["wellyspacing"])
  table = _height_volume_data(_profile(segments, c["welldepth"]))
  return Plate(
    name=name,
    size_x=size_x,
    size_y=size_y,
    size_z=size_z,
    model=f"biomek_{class_name}",
    ordered_items=create_ordered_items_2d(
      Well,
      num_items_x=c["wellsx"],
      num_items_y=rows,
      dx=cm(c["well1xoffset"]) - well_x / 2,
      dy=size_y - cm(c["well1yoffset"]) - (rows - 1) * pitch_y - well_y / 2,
      dz=size_z - depth,
      item_dx=pitch_x,
      item_dy=pitch_y,
      size_x=well_x,
      size_y=well_y,
      size_z=depth,
      bottom_type=BOTTOM_TYPES.get(c.get("bottomsectionshape", ""), WellBottomType.UNKNOWN),
      cross_section_type=(
        CrossSectionType.CIRCLE if c.get("wellstyle") == "Round" else CrossSectionType.RECTANGLE
      ),
      max_volume=c.get("wellvolume") or _capacity(table, depth),
      height_volume_data=table,
      name_prefix=name,
    ),
  )


def _reservoir(class_name: str, c: Dict[str, Any], name: str) -> Plate:
  size_x, size_y, size_z = cm(c["xspan"]), cm(c["yspan"]), cm(c["height"])
  wells: Dict[str, Well] = {}
  for i, s in enumerate(c.get("sections") or []):
    depth = cm(s["depth"])
    table = _height_volume_data(_section_profile(s))
    well = Well(
      name=f"{name}_A{i + 1}",
      size_x=cm(s["x2"] - s["x1"]),
      size_y=cm(s["y2"] - s["y1"]),
      size_z=depth,
      bottom_type=BOTTOM_TYPES.get(s.get("bottomsectionshape", ""), WellBottomType.UNKNOWN),
      cross_section_type=CrossSectionType.RECTANGLE,
      max_volume=s.get("maxvolume") or _capacity(table, depth),
      height_volume_data=table,
    )
    # The section's front left corner: y1/y2 are from the back edge.
    well.location = Coordinate(cm(s["x1"]), size_y - cm(s["y2"]), size_z - depth)
    wells[f"A{i + 1}"] = well
  return Plate(
    name=name,
    size_x=size_x,
    size_y=size_y,
    size_z=size_z,
    model=f"biomek_{class_name}",
    ordered_items=wells,
  )


# -- a well's shape from its segments ----------------------------------------------------
# Biomek describes a well bottom up as segments: a flat floor (no height), a cone from a point, a
# round bottom (a spherical cap), or a frustum from a bottom outline to a top one, round or
# rectangular. A round outline is its radius (`topradius`, `bottomradius`): the `topx`/`topy` of
# some round classes disagree with it, and Biomek's own `sectionvolume` follows the radius (as the
# logged follow and touch-off do: Thermo96VB_PP, 0.4 cm). A reservoir's section is a box.


def _outline(seg: Dict[str, Any], z: float) -> Tuple[float, float]:
  """Half-widths (x, y), cm, of one segment's outline `z` cm above its bottom."""
  height = seg["sectionheight"]
  cross = seg.get("crosssectionshape")
  if seg.get("topshape", "Round") == "Round":
    top = seg.get("topradius", seg.get("topx", 0.0) / 2)
    if cross == "Round":  # a spherical cap: base radius `top`, depth `height`
      sphere = (top * top + height * height) / (2 * height)
      r = math.sqrt(max(0.0, 2 * sphere * z - z * z))
      return r, r
    bottom = 0.0 if cross == "Cone" else seg.get("bottomradius", top)
    r = bottom + (top - bottom) * z / height
    return r, r
  top_x, top_y = seg.get("topx", 0.0) / 2, seg.get("topy", 0.0) / 2
  if cross == "Cone":
    bottom_x = bottom_y = 0.0
  else:
    bottom_x, bottom_y = seg.get("bottomx", 2 * top_x) / 2, seg.get("bottomy", 2 * top_y) / 2
  t = z / height
  return bottom_x + (top_x - bottom_x) * t, bottom_y + (top_y - bottom_y) * t


def _area(seg: Dict[str, Any], z: float) -> float:
  """Cross-section area, cm^2, `z` cm above the segment's bottom."""
  hx, hy = _outline(seg, z)
  return math.pi * hx * hy if seg.get("topshape", "Round") == "Round" else 4 * hx * hy


def _segment_volume(seg: Dict[str, Any], h: float) -> float:
  """Volume, cm^3, of the bottom `h` cm of one segment. The area is quadratic in z for every
  shape, so Simpson's rule is exact."""
  h = min(h, seg["sectionheight"])
  if h <= 0:
    return 0.0
  return h / 6 * (_area(seg, 0) + 4 * _area(seg, h / 2) + _area(seg, h))


@dataclass(frozen=True)
class WellProfile:
  """A well's shape from its bottom up, in Biomek's centimetres: its segments (sections of height
  only), or for a reservoir section its box (`box` = full width x, y)."""

  segments: Tuple[Dict[str, Any], ...] = ()
  box: Optional[Tuple[float, float]] = None
  depth: float = 0.0

  def half_widths(self, z: float) -> Tuple[float, float]:
    """Half-widths (x, y), cm, of the well `z` cm above its bottom (the top's above the top)."""
    if self.box is not None:
      return self.box[0] / 2, self.box[1] / 2
    left = max(0.0, z)
    for seg in self.segments:
      if left <= seg["sectionheight"]:
        return _outline(seg, left)
      left -= seg["sectionheight"]
    return _outline(self.segments[-1], self.segments[-1]["sectionheight"])

  def wall_distance(self, z: float, angle: float) -> float:
    """How far, cm, the wall is from the well's axis `z` cm above its bottom, in direction `angle`
    (radians in Biomek's X/Y): the radius for a round well, to the nearest side for a box."""
    hx, hy = self.half_widths(z)
    if self.box is None and all(s.get("topshape", "Round") == "Round" for s in self.segments):
      return hx
    c, s = abs(math.cos(angle)), abs(math.sin(angle))
    return min(hx / c if c > 1e-12 else math.inf, hy / s if s > 1e-12 else math.inf)

  def volume(self, z: float) -> float:
    """uL held `z` cm above the bottom."""
    if self.box is not None:
      return self.box[0] * self.box[1] * max(0.0, z) * UL_PER_CM3
    left, v = z, 0.0
    for seg in self.segments:
      v += _segment_volume(seg, left)
      left -= seg["sectionheight"]
      if left <= 0:
        break
    return v * UL_PER_CM3

  def height(self, volume: float) -> float:
    """cm of liquid that `volume` uL fills to (bisection; above the top for too much)."""
    if self.box is not None:
      return volume / UL_PER_CM3 / (self.box[0] * self.box[1])
    low, high = 0.0, sum(s["sectionheight"] for s in self.segments)
    while self.volume(high) < volume:
      high *= 2
    for _ in range(60):
      mid = (low + high) / 2
      if self.volume(mid) < volume:
        low = mid
      else:
        high = mid
    return float((low + high) / 2)


def _profile(segments: List[Dict[str, Any]], depth: float) -> Optional[WellProfile]:
  stack = tuple(s for s in segments if (s.get("sectionheight") or 0) > 0)
  return WellProfile(segments=stack, depth=depth) if stack else None


def _section_profile(s: Dict[str, Any]) -> Optional[WellProfile]:
  """A reservoir section's box; None for a shaped bottom (not modelled)."""
  if s.get("bottomsectionshape", "Flat") != "Flat" or s.get("bottomsectiondepth"):
    return None
  return WellProfile(box=(s["x2"] - s["x1"], s["y2"] - s["y1"]), depth=s["depth"])


def well_profile(well: Resource) -> Optional[WellProfile]:
  """The Biomek shape of a well of a plate or reservoir built by `biomek_labware` (from its
  plate's `model`, `biomek_<class>`); None for anything else."""
  parent = well.parent
  model = getattr(parent, "model", None) or ""
  if not model.startswith("biomek_"):
    return None
  c = _classes()["labware"].get(model[len("biomek_") :])
  if c is None:
    return None
  if c.get("type") == "Reservoir":
    sections = c.get("sections") or []
    index = list(parent.children).index(well) if parent is not None else 0
    return _section_profile(sections[index]) if index < len(sections) else None
  return _profile(c.get("segments") or [], c.get("welldepth", 0.0))


def _capacity(table: Optional[Dict[float, float]], depth: float) -> Optional[float]:
  """uL to `depth` mm by the table; None without one (PyLabRobot then takes the well's box)."""
  return None if table is None else interpolate_1d(depth, table, bounds_handling="error")


# mm between a table's heights where a section's cross-section changes with height (a cone, a
# cap): PyLabRobot interpolates between them, within 0.003 mm of Biomek's height for every class.
KNOT_STEP = 0.1


def _height_volume_data(profile: Optional[WellProfile]) -> Optional[Dict[float, float]]:
  """A well's height -> volume table (mm, uL), as PyLabRobot's `height_volume_data` takes it, so
  the well saves and loads as plain data: a point at each section's ends, and every `KNOT_STEP` in
  a section whose cross-section changes, up to the rim (Biomek's volume stops there). None for a
  well with no shape."""
  if profile is None:
    return None
  zs = [0.0]
  if profile.box is None:
    for seg in profile.segments:
      h = seg["sectionheight"]
      constant = _outline(seg, 0.0) == _outline(seg, h) == _outline(seg, h / 2)
      n = 1 if constant else max(1, math.ceil(h * MM_PER_CM / KNOT_STEP))
      zs += [zs[-1] + h * i / n for i in range(1, n + 1)]
  if profile.depth > zs[-1]:
    zs.append(profile.depth)
  return {round(z * MM_PER_CM, 6): profile.volume(z) for z in zs}
