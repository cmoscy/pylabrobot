"""Beckman Coulter Biomek tips.

Transcribed from Biomek's tip classes (Biomek 5, project database `Compliance_Repository`, folder
"Tip Classes"), which state lengths in centimetres:

- `tipheight`: the tip's length; `capacity`: its volume in uL.
- `spanseatingdepth`: how far a mandrel goes into the tip.
- `maxtipradius`: the tip's radius where it seats on the mandrel.
"""

from pylabrobot.resources.tip import Tip


def _biomek_tip(name: str, model: str, length_cm: float, capacity: float) -> Tip:
  return Tip(
    name=name,
    model=model,
    diameter=2 * 0.2413 * 10,  # maxtipradius 0.2413 cm, the same on T230 and T1070
    size_z=length_cm * 10,
    has_filter=False,  # filtered: false
    maximal_volume=capacity,
    nominal_volume=capacity,
    fitting_depth=0.775 * 10,  # spanseatingdepth 0.775 cm
  )


def beckman_coulter_biomek_tip_1070uL(name: str) -> Tip:
  """Biomek T1070 tip, 1070 uL, no filter. Tip class T1070: tipheight 10.9855 cm."""
  return _biomek_tip(name, beckman_coulter_biomek_tip_1070uL.__name__, 10.9855, 1070.0)


def beckman_coulter_biomek_tip_230uL(name: str) -> Tip:
  """Biomek T230 tip, 230 uL, no filter. Tip class T230: tipheight 5.1435 cm."""
  return _biomek_tip(name, beckman_coulter_biomek_tip_230uL.__name__, 5.1435, 230.0)
