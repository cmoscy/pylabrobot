"""A Multichannel pod's 96-channel head: its mandrels and plunger (D). Reached as
`driver.left_pod.head96` (on whichever pod is a Multichannel).

The head's axes (Z, D) are the pod's, moved through `Pod.move_axes`. The host commands here are the
Master Controller's Multichannel ones: `LT` (`features/tip_load.py`) and `HD`. Picking tips up,
dropping them into a rack and discarding them are Biomek's compositions of those, read from the
moves it logged (`tools/validate_head96.py` replays every logged one against these plans):

- pick up: the gripper turned clear (GR to its park angle), mandrel A1 over its tip spot, Z down to
  the seat (the rack's top: on a TL position the rack sits 0.254 cm lower, the ALP's
  `LabwareOffsets`), `LT` with the arguments Biomek derives (`TipLoad.from_configuration`: the
  force scales with the tips loaded), Z up;
- drop into a rack: mandrel A1 over its spot, Z to the seat + `TipUnloadZOffset`, then the eject;
- discard: mandrel A1 at the trash position's `TipDisposalOffsetX`/`Y` from its back left corner,
  the tips' ends `TipDisposalOffsetZ` above it (`resource_model.alps.TipDisposal`), then the eject;
- the eject: the plunger to the bottom of its travel, which drives the ejector, while Z rises by
  the plunger's stroke (so the tips stay put as the mandrels leave them), then the plunger to 0, to
  the bottom again and to 0, at `TipUnloadShuckSpeed`.

Partial loads align as STAR's head does: mandrel `channel` over spot `tip_spot`; only mandrels over
a spot of the rack take a tip (Biomek's 12- and 24-tip loads put mandrel A1 over row H or G).
The head has no tip sensor: which mandrels carry tips is the driver's session state.

A carried tip's end is the head's Z less the tip's effective length, `TipHeight` less the head's
`TipSeatingDepth` (on the 96-head the seating depth is the head's, not
the tip class's, so the `Tip`'s `fitting_depth`, the Span-8's, is not used here).
"""

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

from pylabrobot.beckman_coulter.biomek_i7 import targeting
from pylabrobot.beckman_coulter.biomek_i7.driver.features.homing import PLUNGER_RECIPES, Homer
from pylabrobot.beckman_coulter.biomek_i7.driver.features.tip_load import TipLoad, load_tips
from pylabrobot.beckman_coulter.biomek_i7.resource_model.labware import biomek_tip
from pylabrobot.resources.coordinate import Coordinate
from pylabrobot.resources.resource import Resource
from pylabrobot.resources.tip import Tip
from pylabrobot.resources.tip_rack import TipRack, TipSpot

if TYPE_CHECKING:
  from pylabrobot.beckman_coulter.biomek_i7.deck import BiomekI7Deck
  from pylabrobot.beckman_coulter.biomek_i7.driver.configuration import (
    DeviceConfiguration,
    PodConfiguration,
  )
  from pylabrobot.beckman_coulter.biomek_i7.driver.features.pod import Pod

logger = logging.getLogger(__name__)

ROWS, COLUMNS = 8, 12
DEFAULT_TRASH = "TR1"


def _cm(height: Optional[float]) -> Optional[float]:
  """A height given in mm, as `Pod.move_over` takes it (cm)."""
  return None if height is None else height / 10


def mandrel_name(row: int, column: int) -> str:
  return f"{'ABCDEFGH'[row]}{column + 1}"


def mandrel_index(name: str) -> Tuple[int, int]:
  """(row, column) of a mandrel or well name such as `A1`, `H12`."""
  row, column = "ABCDEFGH".index(name[0].upper()), int(name[1:]) - 1
  if not (0 <= row < ROWS and 0 <= column < COLUMNS):
    raise ValueError(f"no mandrel {name!r} on a 96-channel head")
  return row, column


@dataclass(frozen=True)
class HeadPlan:
  """Where the head goes for a tip operation: mandrel A1's X/Y and the operation's Z, in Biomek's
  frame (cm), and which mandrel meets which tip spot."""

  x: float
  y: float
  z: float
  pairs: Tuple[Tuple[str, TipSpot], ...] = ()


class Head96:
  """The 96-channel head of a Multichannel pod."""

  def __init__(self, pod: "Pod"):
    self._pod = pod
    # The tips on the mandrels, by mandrel name, as this session picked them up.
    self._tips: Dict[str, Tip] = {}

  def __str__(self) -> str:
    return f"{self._pod}'s 96-channel head"

  async def home_plunger(self, force: bool = False) -> None:
    """Home the plunger, the Master Controller's `HD`; `force` as for `Pod.home`."""
    pod = self._pod
    recipe = PLUNGER_RECIPES[pod.module_type]
    logger.info("%s HD%s", pod.component, "" if force else " (if not homed)")
    try:
      await Homer(pod, force=force).run(recipe)
    finally:
      pod._set_home_positions(Homer.axes(recipe))

  async def load_tips(self, load: TipLoad) -> int:
    """Press the head onto tips, the Master Controller's `LT`: see `features/tip_load.py`, and
    `TipLoad.from_configuration` for the arguments Biomek would send. Returns the loaded Z, in
    counts.

    Raises:
      ValueError: for arguments the Master Controller refuses; nothing is sent.
    """
    return await load_tips(self._pod, load)

  # -- tips ---------------------------------------------------------------------------------------

  @property
  def tips(self) -> List[Optional[str]]:
    """Each mandrel's tip (its model), A1..H12 row by row; None for none. Kept in the session state:
    the head cannot sense tips."""
    driver = self._pod.driver
    if driver is None:
      return [None] * (ROWS * COLUMNS)
    tips = driver.state.pod(self._pod.side).tips
    return list(tips) + [None] * (ROWS * COLUMNS - len(tips))

  def _set_tips(self, mandrels: Dict[str, Optional[str]]) -> None:
    driver = self._pod.driver
    if driver is None:
      return
    state = driver.state.pod(self._pod.side)
    tips = self.tips
    for name, model in mandrels.items():
      row, column = mandrel_index(name)
      tips[row * COLUMNS + column] = model
    state.tips = tips
    if driver.state_json:
      driver.save_state()

  def _context(self) -> Tuple["BiomekI7Deck", "DeviceConfiguration", "PodConfiguration"]:
    driver, declared = self._pod.driver, self._pod.declared
    if driver is None or driver.deck is None or declared is None:
      raise RuntimeError(f"{self} needs a deck and a declared configuration (use `BiomekI7`)")
    return driver.deck, driver.declared, declared

  def _pairs(self, rack: TipRack, channel: str, tip_spot: str) -> Tuple[Tuple[str, TipSpot], ...]:
    """Each mandrel over one of `rack`'s spots, with mandrel `channel` over spot `tip_spot`."""
    cr, cc = mandrel_index(channel)
    sr, sc = mandrel_index(tip_spot)
    pairs = []
    for r in range(ROWS):
      for c in range(COLUMNS):
        rr, rc = r - cr + sr, c - cc + sc
        if 0 <= rr < rack.num_items_y and 0 <= rc < rack.num_items_x:
          pairs.append((mandrel_name(r, c), rack.get_item(mandrel_name(rr, rc))))
    return tuple(pairs)

  def _a1_over(
    self, rack: TipRack, channel: str, tip_spot: str, offset: Coordinate
  ) -> Tuple[float, float]:
    """Mandrel A1's X/Y (Biomek cm) with mandrel `channel` over spot `tip_spot`."""
    deck, configuration, declared = self._context()
    spot = targeting.target(
      deck, configuration, self._pod.pod_number, rack.get_item(tip_spot), offset=offset
    )
    cr, cc = mandrel_index(channel)
    geometry = declared.geometry
    return spot.x - cc * geometry.mandrel_x_spacing, spot.y - cr * geometry.mandrel_y_spacing

  def _seat(self, rack: TipRack) -> float:
    """The seat: the rack's top (the position's Z + its offset for the rack's class + the class's
    `Height`), as the logged tip loads put it."""
    deck, configuration, _ = self._context()
    return targeting.target(deck, configuration, self._pod.pod_number, rack, z="t").z

  def effective_tip_length(self, tip: Tip) -> float:
    """How far below the mandrels `tip`'s end is, cm: its length less the head's `TipSeatingDepth`
    (as the logged tip moves confirm)."""
    if self._pod.declared is None:
      raise RuntimeError(f"{self} needs a declared configuration")
    return float(tip.get_size_z() / 10 - self._pod.declared.settings["TipSeatingDepth"])

  def plan_pick_up(
    self,
    tip_rack: TipRack,
    offset: Coordinate = Coordinate.zero(),
    channel: str = "A1",
    tip_spot: str = "A1",
  ) -> Tuple[HeadPlan, TipLoad]:
    """Where a pickup goes, and the `LT` it sends. Only spots holding a tip (when tracked) count.

    Raises:
      ValueError: if no mandrel would meet a tip.
    """
    _, _, declared = self._context()
    pairs = tuple(
      (m, s) for m, s in self._pairs(tip_rack, channel, tip_spot) if not s.tracks_tips or s.tip
    )
    if not pairs:
      raise ValueError(f"{self}: no tips under the head on {tip_rack.name}")
    x, y = self._a1_over(tip_rack, channel, tip_spot, offset)
    seat = self._seat(tip_rack)
    plan = HeadPlan(x, y, seat, pairs)
    return plan, TipLoad.from_configuration(declared, seat, len(pairs))

  def plan_drop(
    self,
    tip_rack: TipRack,
    offset: Coordinate = Coordinate.zero(),
    channel: str = "A1",
    tip_spot: str = "A1",
  ) -> HeadPlan:
    """Where a drop into `tip_rack` goes: the seat + `TipUnloadZOffset`."""
    _, _, declared = self._context()
    x, y = self._a1_over(tip_rack, channel, tip_spot, offset)
    z = self._seat(tip_rack) + declared.settings["TipUnloadZOffset"]
    return HeadPlan(x, y, z, self._pairs(tip_rack, channel, tip_spot))

  def plan_discard(self, tip_length: float, trash: Optional[Resource] = None) -> HeadPlan:
    """Where a discard of tips `tip_length` cm long (effective: `effective_tip_length`) goes:
    mandrel A1 at the trash position's `TipDisposal` X/Y from its back left corner, the tips' ends
    its Z above it, as the logged discards put it.

    Raises:
      ValueError: for a position that is not a trash (no `tip_disposal`).
    """
    deck, configuration, _ = self._context()
    position = trash if trash is not None else deck.get_position(DEFAULT_TRASH)
    disposal = getattr(position, "tip_disposal", None)
    if disposal is None:
      raise ValueError(f"{position.name} is not a trash position (no tip disposal offsets)")
    corner = targeting.target(
      deck, configuration, self._pod.pod_number, position, x="l", y="b", z="b"
    )
    z = corner.z + tip_length + disposal.z
    return HeadPlan(corner.x + disposal.x, corner.y + disposal.y, z)

  def _tip_length(self, mandrels: List[str]) -> float:
    """The longest carried tip's effective length, cm: from the tips picked up this session, or
    from the tip class the session state names (`biomek_<class>`)."""
    lengths = []
    for i, model in enumerate(self.tips):
      name = mandrel_name(i // COLUMNS, i % COLUMNS)
      if name not in mandrels:
        continue
      tip = self._tips.get(name)
      if tip is not None:
        lengths.append(self.effective_tip_length(tip))
      elif model and model.startswith("biomek_"):
        lengths.append(self.effective_tip_length(biomek_tip(model[len("biomek_") :], "t")))
    if not lengths:
      raise RuntimeError(f"{self}: the carried tips' length is not known")
    return max(lengths)

  def carried_tip_lengths(self) -> Dict[str, float]:
    """`{"Z": cm}`: how far below the mandrels the longest carried tip ends; empty without tips.

    Raises:
      RuntimeError: for carried tips whose length is not known.
    """
    carried = [
      mandrel_name(i // COLUMNS, i % COLUMNS) for i, t in enumerate(self.tips) if t is not None
    ]
    return {"Z": self._tip_length(carried)} if carried else {}

  async def pick_up_tips(
    self,
    tip_rack: TipRack,
    offset: Optional[Coordinate] = None,
    minimum_traverse_height_start: Optional[float] = None,
    minimum_traverse_height_end: Optional[float] = None,
    *,
    channel: str = "A1",
    tip_spot: str = "A1",
  ) -> None:
    """Pick up tips from `tip_rack` with mandrel `channel` over spot `tip_spot` (STAR's `Head96`
    signature). Every mandrel over a spot holding a tip takes it. Heights are mm above the deck of
    the head's lowest point (the tips' ends when it carries tips, else the mandrels), as STAR's;
    None travels and leaves the head as high as it goes.

    Raises:
      RuntimeError: if the head already carries tips, or there is no deck.
      ValueError: if no mandrel would meet a tip.
    """
    if any(t is not None for t in self.tips):
      raise RuntimeError(f"{self} already carries tips; drop or discard them first")
    plan, load = self.plan_pick_up(tip_rack, offset or Coordinate.zero(), channel, tip_spot)
    _, _, declared = self._context()
    pod = self._pod
    logger.info("%s pick up %d tips from %s", pod.component, len(plan.pairs), tip_rack.name)
    await self._clear_gripper()
    await pod.move_over(
      {"X": declared.to_counts("X", plan.x), "Y": declared.to_counts("Y", plan.y)},
      minimum_traverse_height=_cm(minimum_traverse_height_start),
    )
    await pod.move_axes({"Z": declared.to_counts("Z", plan.z)}, 1000)
    await self.load_tips(load)
    picked = {}
    for mandrel, spot in plan.pairs:
      tip = spot.tip_for_pickup()
      if spot.tracks_tips:
        spot.unassign_tip()
      self._tips[mandrel] = tip
      picked[mandrel] = tip.model
    self._set_tips(picked)  # type: ignore[arg-type]
    await pod.lift_channels(_cm(minimum_traverse_height_end))

  async def drop_tips(
    self,
    tip_rack: TipRack,
    offset: Optional[Coordinate] = None,
    minimum_traverse_height_start: Optional[float] = None,
    minimum_traverse_height_end: Optional[float] = None,
    *,
    channel: str = "A1",
    tip_spot: str = "A1",
  ) -> None:
    """Put the head's tips into `tip_rack`, mandrel `channel` over spot `tip_spot`, and eject them.
    Heights as `pick_up_tips`.

    Raises:
      RuntimeError: if the head carries no tips, or there is no deck.
      ValueError: if a mandrel with a tip would meet an occupied spot, or none would meet a spot.
    """
    carried = self._carried()
    plan = self.plan_drop(tip_rack, offset or Coordinate.zero(), channel, tip_spot)
    targets = [(m, s) for m, s in plan.pairs if m in carried]
    if not targets:
      raise ValueError(f"{self}: no mandrel with a tip is over {tip_rack.name}")
    occupied = [s.name for _, s in targets if s.tracks_tips and s.tip is not None]
    if occupied:
      raise ValueError(f"{self}: tip spots {occupied} already hold tips")
    await self._eject_at(plan, minimum_traverse_height_start)
    for mandrel, spot in targets:
      tip = self._tips.pop(mandrel, None)
      if spot.tracks_tips:
        spot.assign_tip(tip if tip is not None else spot.make_tip())
    self._set_tips({m: None for m in carried})
    await self._pod.lift_channels(_cm(minimum_traverse_height_end))

  async def discard_tips(
    self,
    trash: Optional[Resource] = None,
    minimum_traverse_height_start: Optional[float] = None,
    minimum_traverse_height_end: Optional[float] = None,
  ) -> None:
    """Eject the head's tips into the trash (`TR1` by default). Heights as `pick_up_tips`.

    Raises:
      RuntimeError: if the head carries no tips, or there is no deck.
    """
    carried = self._carried()
    plan = self.plan_discard(self._tip_length(carried), trash)
    await self._eject_at(plan, minimum_traverse_height_start)
    for mandrel in carried:
      self._tips.pop(mandrel, None)
    self._set_tips({m: None for m in carried})
    await self._pod.lift_channels(_cm(minimum_traverse_height_end))

  def _carried(self) -> List[str]:
    carried = [
      mandrel_name(i // COLUMNS, i % COLUMNS) for i, t in enumerate(self.tips) if t is not None
    ]
    if not carried:
      raise RuntimeError(f"{self} carries no tips")
    return carried

  async def _eject_at(self, plan: HeadPlan, start: Optional[float]) -> None:
    _, _, declared = self._context()
    pod = self._pod
    logger.info("%s eject tips at X %.3f Y %.3f Z %.3f cm", pod.component, plan.x, plan.y, plan.z)
    await self._clear_gripper()
    await pod.move_over(
      {"X": declared.to_counts("X", plan.x), "Y": declared.to_counts("Y", plan.y)},
      minimum_traverse_height=_cm(start),
    )
    await pod.move_axes({"Z": declared.to_counts("Z", plan.z)}, 1000)
    await self.eject()

  async def eject(self) -> None:
    """Push the tips off the mandrels where the head is: the plunger to the bottom of its travel
    (which drives the ejector) while Z rises by the plunger's stroke, so the tips stay where they
    are as the mandrels leave them; then the plunger to 0, the bottom and 0 again; all at
    `TipUnloadShuckSpeed`."""
    _, _, declared = self._context()
    pod = self._pod
    bottom = declared.limits["D"][0]  # uL, below 0: the ejector stroke
    stroke = -bottom / declared.settings["DAxisULPerCM"]  # cm the plunger travels
    speed = round(declared.settings["TipUnloadShuckSpeed"] * 10)
    z = declared.to_value("Z", pod.cpositions["Z"][1])
    d_bottom, d_zero = declared.to_counts("D", bottom), declared.to_counts("D", 0.0)
    await pod.move_axes({"Z": declared.to_counts("Z", z + stroke), "D": d_bottom}, speed)
    for target in (d_zero, d_bottom, d_zero):
      await pod.move_axes({"D": target}, speed)

  async def _clear_gripper(self) -> None:
    """Turn the gripper to its park angle (GR), as Biomek does before the head works the deck."""
    _, _, declared = self._context()
    gr = declared.to_counts("GR", declared.park["GR"])
    if self._pod.cpositions["GR"][1] != gr:
      await self._pod.move_axes({"GR": gr})
