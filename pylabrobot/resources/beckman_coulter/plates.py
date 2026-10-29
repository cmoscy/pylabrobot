"""Beckman Coulter plates, as Biomek defines them.

Transcribed from Biomek's labware classes (Biomek 5, project database `Compliance_Repository`,
folder "Labware Classes"), which state lengths in centimetres:

- `xspan`, `yspan`, `height`: the plate's footprint and height.
- `wellsx`, `wellsy`, `wellxspacing`, `wellyspacing`: the well grid.
- `well1xoffset`, `well1yoffset`: well A1's centre, from the plate's left and back edges.
- `wellradius`, `welldepth`, `wellvolume`, `bottomsectionshape`: a well's top radius, depth,
  capacity and bottom.
"""

from pylabrobot.resources.plate import Plate
from pylabrobot.resources.utils import create_ordered_items_2d
from pylabrobot.resources.well import CrossSectionType, Well, WellBottomType


def beckman_coulter_96_wellplate_362uL_Fb(name: str) -> Plate:
  """Beckman Coulter 96-well microplate, round flat-bottomed wells. Part no. 609844.

  Labware class BCFlat96.
  """
  size_x, size_y, size_z = 127.762, 85.471, 14.224  # xspan 12.7762, yspan 8.5471, height 1.4224 cm
  well_diameter = 6.86  # wellradius 0.343 cm
  well_depth = 10.668  # welldepth 1.0668 cm
  return Plate(
    name=name,
    size_x=size_x,
    size_y=size_y,
    size_z=size_z,
    model=beckman_coulter_96_wellplate_362uL_Fb.__name__,
    ordered_items=create_ordered_items_2d(
      Well,
      num_items_x=12,  # wellsx
      num_items_y=8,  # wellsy
      dx=14.38 - well_diameter / 2,  # well1xoffset 1.438 cm
      # The front row's well, from the front edge: A1 (well1yoffset 1.123 cm) is in the back row.
      dy=size_y - 11.23 - 7 * 9.0 - well_diameter / 2,
      dz=size_z - well_depth,
      item_dx=9.0,  # wellxspacing 0.9 cm
      item_dy=9.0,  # wellyspacing 0.9 cm
      size_x=well_diameter,
      size_y=well_diameter,
      size_z=well_depth,
      bottom_type=WellBottomType.FLAT,  # bottomsectionshape Flat
      cross_section_type=CrossSectionType.CIRCLE,  # wellstyle Round
      max_volume=362.76,  # wellvolume, uL
      name_prefix=name,
    ),
  )
