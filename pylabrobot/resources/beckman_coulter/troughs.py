"""Beckman Coulter reservoirs, as Biomek defines them.

Transcribed from Biomek's labware classes (Biomek 5, project database `Compliance_Repository`,
folder "Labware Classes"), which state lengths in centimetres. A reservoir's class gives its
footprint and `height`, and one section per cavity: its extent (`x1`..`x2` from the left edge,
`y1`..`y2` from the back edge), `depth` from the top, the depth of its shaped bottom and its
capacity. It does not give wall or floor thickness, so the cavity's floor is placed at
`height - depth` and the material under it is not split out.
"""

from pylabrobot.resources.trough import Trough, TroughBottomType


def beckman_coulter_1_trough_200mL_Vb(name: str) -> Trough:
  """Beckman Coulter reservoir with baffles, for bulk reagents dispensed by the Span-8. Part no. 372784.

  Labware class BCFullReservoir: one section, x 0.1524..12.827 cm, y 0.1524..8.5344 cm, depth
  2.159 cm with a 1.117 cm V-shaped bottom, 200 mL.
  """
  height = 38.1  # height 3.81 cm
  return Trough(
    name=name,
    size_x=129.79,  # xspan 12.979 cm
    size_y=85.852,  # yspan 8.5852 cm
    size_z=height,
    max_volume=200_000,  # sections[0].maxvolume, uL
    material_z_thickness=0,
    through_base_to_container_base=height - 21.59,  # sections[0].depth 2.159 cm
    model=beckman_coulter_1_trough_200mL_Vb.__name__,
    bottom_type=TroughBottomType.V,  # sections[0].bottomsectionshape V
  )
