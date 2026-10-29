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
  `bottomsectionshape` / `wellstyle`, and height <-> volume from the well's segments;
- a `Reservoir` is a `Plate` whose wells are its sections (`x1..x2`, `y1..y2` from the left and
  back edges, `depth`).

Lids, tube racks and reservations are not built yet.
"""

import functools
import json
import math
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from pylabrobot.resources.coordinate import Coordinate
from pylabrobot.resources.plate import Plate
from pylabrobot.resources.tip import Tip
from pylabrobot.resources.tip_rack import StandingTipRack, TipSpot
from pylabrobot.resources.utils import create_ordered_items_2d
from pylabrobot.resources.well import CrossSectionType, Well, WellBottomType

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
  volume = _volume_functions(segments)
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
      max_volume=c.get("wellvolume") or None,
      compute_volume_from_height=volume[0] if volume else None,
      compute_height_from_volume=volume[1] if volume else None,
      name_prefix=name,
    ),
  )


def _reservoir(class_name: str, c: Dict[str, Any], name: str) -> Plate:
  size_x, size_y, size_z = cm(c["xspan"]), cm(c["yspan"]), cm(c["height"])
  wells: Dict[str, Well] = {}
  for i, s in enumerate(c.get("sections") or []):
    depth = cm(s["depth"])
    well = Well(
      name=f"{name}_A{i + 1}",
      size_x=cm(s["x2"] - s["x1"]),
      size_y=cm(s["y2"] - s["y1"]),
      size_z=depth,
      bottom_type=BOTTOM_TYPES.get(s.get("bottomsectionshape", ""), WellBottomType.UNKNOWN),
      cross_section_type=CrossSectionType.RECTANGLE,
      max_volume=s.get("maxvolume") or None,
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


# -- a well's volume from its segments ---------------------------------------------------
# Biomek describes a well bottom up as segments: a flat floor (no height), a cone from a point, or a
# frustum from a bottom outline to a top one, round or rectangular. Volume is integrated over them.


def _area(shape: str, x: float, y: float) -> float:
  """Cross-section area, cm^2, of an outline `x` by `y` cm (a round one's x is its diameter)."""
  return math.pi * x * y / 4 if shape == "Round" else x * y


def _segment_volume(seg: Dict[str, Any], h: float) -> float:
  """Volume, cm^3, of the bottom `h` cm of one segment."""
  height = seg.get("sectionheight") or 0.0
  if height <= 0 or h <= 0:
    return 0.0
  h = min(h, height)
  top_x, top_y = seg.get("topx", 0.0), seg.get("topy", 0.0)
  shape = seg.get("topshape", "Round")
  if seg.get("crosssectionshape") == "Cone" or "bottomx" not in seg:
    bottom_x = bottom_y = 0.0
  else:
    bottom_x, bottom_y = seg["bottomx"], seg.get("bottomy", seg["bottomx"])

  def area(z: float) -> float:
    t = z / height
    return _area(shape, bottom_x + (top_x - bottom_x) * t, bottom_y + (top_y - bottom_y) * t)

  # The area is quadratic in z: Simpson's rule is exact.
  return h / 6 * (area(0) + 4 * area(h / 2) + area(h))


def _volume_functions(
  segments: List[Dict[str, Any]],
) -> Optional[Tuple[Callable[[float], float], Callable[[float], float]]]:
  """(volume from height, height from volume), PyLabRobot's units (mm, uL), or None for a well
  with no segments of height."""
  stack = [s for s in segments if (s.get("sectionheight") or 0) > 0]
  if not stack:
    return None
  total_height = float(sum(s["sectionheight"] for s in stack))

  def volume(height_mm: float) -> float:
    left, v = height_mm / MM_PER_CM, 0.0
    for seg in stack:
      v += _segment_volume(seg, left)
      left -= seg["sectionheight"]
      if left <= 0:
        break
    return v * UL_PER_CM3

  def height(volume_ul: float) -> float:
    low, high = 0.0, total_height * MM_PER_CM
    for _ in range(60):
      mid = (low + high) / 2
      if volume(mid) < volume_ul:
        low = mid
      else:
        high = mid
    return (low + high) / 2

  return volume, height
