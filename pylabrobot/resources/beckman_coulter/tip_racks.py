"""Beckman Coulter Biomek tip boxes.

Transcribed from Biomek's labware classes (Biomek 5, project database `Compliance_Repository`,
folder "Labware Classes"), which state lengths in centimetres:

- `xspan`, `yspan`, `height`: the box's footprint and height, tips in.
- `wellsx`, `wellsy`, `wellxspacing`, `wellyspacing`: the tip grid.
- `well1xoffset`, `well1yoffset`: tip A1's centre, from the box's left and back edges.
- `wellradius`: the radius of the hole a tip stands in.

The tips' tops are at the box's `height`, where the head's mandrels meet them, so a tip spot sits
on the top face, as on a nested rack.
"""

from pylabrobot.resources.tip import TipCreator
from pylabrobot.resources.tip_rack import StandingTipRack, TipSpot
from pylabrobot.resources.utils import create_ordered_items_2d

from .tips import beckman_coulter_biomek_tip_230uL, beckman_coulter_biomek_tip_1070uL


def _biomek_96_tip_box(
  name: str,
  model: str,
  height_cm: float,
  well1_x_cm: float,
  make_tip: TipCreator,
  with_tips: bool,
) -> StandingTipRack:
  size_x, size_y = 127.9, 85.4  # xspan 12.79 cm, yspan 8.54 cm
  pitch = 9.0  # wellxspacing, wellyspacing 0.9 cm
  hole = 2 * 2.8  # wellradius 0.28 cm
  well1_y = 11.2  # well1yoffset 1.12 cm, from the back edge
  return StandingTipRack(
    name=name,
    size_x=size_x,
    size_y=size_y,
    size_z=height_cm * 10,
    model=model,
    ordered_items=create_ordered_items_2d(
      TipSpot,
      num_items_x=12,  # wellsx
      num_items_y=8,  # wellsy
      dx=well1_x_cm * 10 - hole / 2,
      # The front row's hole, from the front edge: A1 is in the back row.
      dy=size_y - well1_y - 7 * pitch - hole / 2,
      dz=height_cm * 10,
      item_dx=pitch,
      item_dy=pitch,
      size_x=hole,
      size_y=hole,
      make_tip=make_tip,
      name_prefix=name,
    ),
    with_tips=with_tips,
  )


def beckman_coulter_biomek_96_tiprack_1070uL(name: str, with_tips: bool = True) -> StandingTipRack:
  """Biomek BC1070 tip box: 96 T1070 tips. Part no. B85945 (sterile), B85940 (non-sterile).

  Labware class BC1070: height 11.41 cm, well1xoffset 1.44 cm.
  """
  return _biomek_96_tip_box(
    name,
    beckman_coulter_biomek_96_tiprack_1070uL.__name__,
    height_cm=11.41,
    well1_x_cm=1.44,
    make_tip=beckman_coulter_biomek_tip_1070uL,
    with_tips=with_tips,
  )


def beckman_coulter_biomek_96_tiprack_230uL(name: str, with_tips: bool = True) -> StandingTipRack:
  """Biomek BC230 tip box: 96 T230 tips. Part no. B85906 (sterile), B85903 (non-sterile).

  Labware class BC230: height 5.59 cm, well1xoffset 1.45 cm.
  """
  return _biomek_96_tip_box(
    name,
    beckman_coulter_biomek_96_tiprack_230uL.__name__,
    height_cm=5.59,
    well1_x_cm=1.45,
    make_tip=beckman_coulter_biomek_tip_230uL,
    with_tips=with_tips,
  )
