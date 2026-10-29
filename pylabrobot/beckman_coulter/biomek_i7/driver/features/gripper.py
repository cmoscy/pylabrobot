"""A pod's gripper: the four gripper axes (GY, GG, GZ, GR) every pod carries. Reached as
`driver.left_pod.gripper` / `driver.right_pod.gripper`.

The gripper's axes are the pod's: it moves through `Pod.move_axes` with the rest of the pod (a grip
is GG moved force-limited), and what it holds widens the pod's range on the X rail. This feature
holds what is the gripper's own: checking a grip, and telling the rail what is held.
"""

import logging
from typing import TYPE_CHECKING, List, Literal, Optional

from pylabrobot.beckman_coulter.biomek_i7.driver import pod_geometry as g
from pylabrobot.beckman_coulter.biomek_i7.driver import protocol as p
from pylabrobot.beckman_coulter.biomek_i7.driver.errors import GripCheckError
from pylabrobot.resources.resource import Resource

if TYPE_CHECKING:
  from pylabrobot.beckman_coulter.biomek_i7.driver.features.pod import Pod

logger = logging.getLogger(__name__)

# GG's control effort below this means the gripper holds labware, as the Master Controller judges
# a grip (`AG` / `AU`).
GRIPPED_LABWARE_LIMIT = -200


MM_PER_CM = 10.0


def held_labware_of(
  resource: Resource,
  across: Literal["x", "y"] = "y",
  offset_x: float = 0.0,
  offset_y: float = 0.0,
) -> g.HeldLabware:
  """What the rail counts for `resource` held by the gripper: its footprint, and the lid on it as a
  stacked layer (as Biomek counts a gripped stack), in cm.

  Args:
    resource: what is held. A lid seated on it (`resource.lid`) is the layer above it.
    across: the resource's axis between the fingers: `y` when the fingers close on its front and
      back, `x` on its sides. The other runs along the fingers.
    offset_x, offset_y: how far the resource's centre is from the pads' centre, along and across
      the fingers, cm (Biomek's `GrippedLabwareOffsets`).
  """

  def footprint(r: Resource) -> List[float]:
    size = [r.get_size_x() / MM_PER_CM, r.get_size_y() / MM_PER_CM]
    return size if across == "y" else size[::-1]

  length, width = footprint(resource)
  layers = []
  lid = getattr(resource, "lid", None)
  if lid is not None and lid.location is not None:
    corner = [lid.location.x / MM_PER_CM, lid.location.y / MM_PER_CM]
    dx, dy = corner if across == "y" else corner[::-1]
    layers.append(g.StackedLayer(dx, dy, *footprint(lid)))
  return g.HeldLabware(length, width, offset_x, offset_y, tuple(layers))


class Gripper:
  """The gripper on one pod."""

  def __init__(self, pod: "Pod"):
    self._pod = pod

  def __str__(self) -> str:
    return f"{self._pod}'s gripper"

  @property
  def held_labware(self) -> Optional[g.HeldLabware]:
    """What the gripper holds, as the rail counts it; None for nothing. The pod cannot sense it:
    it is what was last set."""
    return self._pod._held_labware

  def set_held_labware(self, held: Optional[g.HeldLabware]) -> None:
    """Tell the rail what the gripper now holds (None: nothing), so the pod's range counts it."""
    self._pod._held_labware = held
    self._pod._positions_changed()

  async def request_control_effort(self, axis: str = "GG") -> int:
    """How hard a motor is working to hold its position, in the drive's units. Moves nothing."""
    reply = await self._pod.board.send(p.get_control_effort(self._pod.axis(axis).index))
    return p.parse_s32(reply.payload)

  async def check_grip(self, expect_labware: bool) -> None:
    """Check that the gripper does (`expect_labware`) or does not hold labware, from GG's control
    effort: the Master Controller's `AG` / `AU`. Moves nothing.

    Raises:
      GripCheckError: if it does not match.
    """
    effort = await self.request_control_effort("GG")
    holds = effort < GRIPPED_LABWARE_LIMIT
    if holds != expect_labware:
      what = "holds no labware" if expect_labware else "still holds labware"
      raise GripCheckError(f"{self._pod}: the gripper {what} (GG control effort {effort})")
