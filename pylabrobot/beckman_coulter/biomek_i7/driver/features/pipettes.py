"""A Span-8 pod's eight independent channels: their syringe pumps, valves, liquid-level sensors and
tip sensor. Reached as `driver.right_pod.pipettes` (on whichever pod is a Span-8).

The channels' axes (Y1-Y8, Z1-Z8, D1-D8) are the pod's, moved through `Pod.move_axes`. The pumps'
valves and cutoff velocity are kept in the pod's state, because every pump move carries them;
this feature is what sets them. The host commands here are the Master Controller's Span-8 ones, as
its logs show them: `CV`, `VI`/`VO`/`VB`/`VE`, `SS`, `GT`, `VT`.

Picking tips up and discarding them are Biomek's compositions of plain moves, as its logs show
them, replayed against every logged one:

- pick up, per set of spots the probes reach in one X/Y/S move (one rack, one X, evenly spaced in
  Y): probe 1 over its spot with the span the spots need; the probes used, and every other probe
  without a tip over an emptied spot of the rack (tip tracking), down to an approach over the rack's top (`AdditionalSafeHeight` +
  `AvoidanceMargin`, Biomek's safe height there, in its world model: by pod 1's corner of the
  position); then pressed in groups (the first probe still waiting and later ones
  at least `MinLoadingInterval` from the last taken, up to `MaxLoadingProbes`: 1+5, 2+6, 3+7, 4+8).
  One move per group at `LoadSpeed` sends the group to the seat (the rack's top + `TipLoadZOffset`
  + the probe's `LoadingOffsets`) and the group before it up to the exit (the top + 1.0 cm); the
  last group then rises alone. `TiploadExitZOffset` and `TipLoadTime` are declared, but no logged
  move depends on either;
- discard: probe 1 at the trash's `XSpan`/2 + `Span8TipDisposalOffsetX`, the probes centred in Y
  ((`YSpan` - span)/2 + `Span8TipDisposalOffsetY`) at the narrowest span, the probes at
  `DiscardHeight` - 1.0 cm; then shucked in groups (the first probe still to go and
  later ones at least `MinUnloadingInterval` from the last taken, up to `MaxUnloadingProbes`): each
  probe of a group driven to the top of its travel + `DiscardOvershoot` at `DiscardSpeed`, where
  the pod's stripper pushes the tip off, and back;
- with `DetectTips` set, the tip sensor then checks the probes worked (`VT`).

The stripper works only at the top of travel, so a Span-8 cannot put tips back into a rack: they go
to waste. Carrying probes travel no higher than the top less `DiscardOffset`
(`Pod.traverse_ceilings`). Which probes carry which tips is the session state.
"""

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, NamedTuple, Optional, Sequence, Tuple

from pylabrobot.beckman_coulter.biomek_i7 import targeting
from pylabrobot.beckman_coulter.biomek_i7.driver import motion
from pylabrobot.beckman_coulter.biomek_i7.driver import protocol as p
from pylabrobot.beckman_coulter.biomek_i7.driver.errors import TipCheckError
from pylabrobot.beckman_coulter.biomek_i7.resource_model.labware import biomek_tip
from pylabrobot.resources.coordinate import Coordinate
from pylabrobot.resources.errors import HasTipError
from pylabrobot.resources.resource import Resource
from pylabrobot.resources.tip import Tip
from pylabrobot.resources.tip_rack import TipSpot

if TYPE_CHECKING:
  from pylabrobot.beckman_coulter.biomek_i7.deck import BiomekI7Deck
  from pylabrobot.beckman_coulter.biomek_i7.driver.configuration import (
    DeviceConfiguration,
    PodConfiguration,
  )
  from pylabrobot.beckman_coulter.biomek_i7.driver.features.pod import Pod

logger = logging.getLogger(__name__)

# The valves' Move All timeout, as the Master Controller's logs show it.
VALVE_MOVE_ALL_TIMEOUT_MS = 6000

# Cutoff velocity range (`CV`), counts/s; quantized to 4.
CUTOFF_VELOCITY_RANGE = (200, 10800)

PROBES = 8
DEFAULT_TRASH = "TR2"  # the right trash, where Biomek discards the Span-8's tips
EXIT_ABOVE_TOP = 1.0  # cm: the probes leave a pickup this far above the rack (logged)
DISCARD_BELOW = 1.0  # cm: the probes shuck from `DiscardHeight` less this (logged)
# How far apart, cm, two spots may be in X, or off the probes' even spacing in Y, for one move.
ALIGNMENT_TOLERANCE = 0.005


def press_groups(probes: Sequence[int], most: int, interval: int) -> List[Tuple[int, ...]]:
  """Biomek's grouping of probes for pressing onto tips or shucking them, as its logs show it: each
  group takes the first probe still waiting, then later ones at least
  `interval` probes from the last one taken, up to `most`."""
  waiting = sorted(probes)
  groups = []
  while waiting:
    group = [waiting.pop(0)]
    for probe in list(waiting):
      if len(group) < most and probe - group[-1] >= interval:
        group.append(probe)
        waiting.remove(probe)
    groups.append(tuple(group))
  return groups


class _Job(NamedTuple):
  """A probe and its tip spot, with where the probe must be (Biomek cm), the rack's top for this
  pod, the rack's top in Biomek's world model (pod 1's corner of the position) and the rack's
  spots with where each is (X, Y)."""

  probe: int
  spot: TipSpot
  x: float
  y: float
  top: float
  world_top: float
  rack_spots: Tuple[Tuple[TipSpot, float, float], ...]


@dataclass(frozen=True)
class PickUpPlan:
  """One X/Y/S move's pickup, in Biomek's frame (cm): probe 1's X/Y, the span from probe 1 to 8,
  which probe meets which spot, the heights (the probes' bottoms), the probes lowered to the
  approach (those picking up, and those without tips over the rack) and the press groups."""

  x: float
  y: float
  span: float
  pairs: Tuple[Tuple[int, TipSpot], ...]
  approach: float
  lowered: Tuple[int, ...]
  seats: Dict[int, float]
  exit: float
  groups: Tuple[Tuple[int, ...], ...]


@dataclass(frozen=True)
class DiscardPlan:
  """A discard, in Biomek's frame (cm): probe 1's X/Y, the span, the probes' Z before the shuck,
  the shuck groups and each probe's shuck target."""

  x: float
  y: float
  span: float
  z: float
  groups: Tuple[Tuple[int, ...], ...]
  shuck: Dict[int, float]


def tip_problems(states: Sequence[Optional[str]], expected: Mapping[int, bool]) -> List[str]:
  """What the Master Controller's `VT` reports, in its words: per probe (1..8) whose tip is not as
  `expected` says, in probe order. A state of `loaded` is a tip, `unknown` cannot be told, and
  anything else is no tip."""
  problems = []
  for probe, should in sorted(expected.items()):
    state = states[probe - 1]
    if state == "unknown":
      problems.append(f"The system cannot determine whether or not probe {probe} has a tip on.")
    elif state == "loaded" and not should:
      problems.append(f"Probe {probe} should not have a tip on, but it does.")
    elif state != "loaded" and should:
      problems.append(f"Probe {probe} should have a tip on, but it does not.")
  return problems


class Pipettes:
  """The eight channels of a Span-8 pod."""

  def __init__(self, pod: "Pod"):
    self._pod = pod
    # The tips on the probes, by probe (1..8), as this session picked them up.
    self._tips: Dict[int, Tip] = {}

  def __str__(self) -> str:
    return f"{self._pod}'s channels"

  # -- pumps -------------------------------------------------------------------------

  @property
  def cutoff_velocity(self) -> int:
    """The pumps' cutoff velocity for the moves that follow, counts/s (0 until set)."""
    return self._pod._cutoff_velocity

  def set_cutoff_velocity(self, velocity: int) -> None:
    """The syringe pumps' cutoff velocity for the moves that follow, counts/s: the Master
    Controller's `CV`, quantized to 4 as it does. Sends nothing.

    Raises:
      ValueError: outside 200..10800.
    """
    low, high = CUTOFF_VELOCITY_RANGE
    if not low <= velocity <= high:
      raise ValueError(f"cutoff velocity {velocity} is outside {low}..{high}")
    self._pod._cutoff_velocity = motion.quantize(velocity, 4.0)

  @property
  def valves(self) -> Dict[str, Optional[p.Valve]]:
    """Each syringe pump's valve, as this driver last set it; None until set."""
    return dict(self._pod._valves)

  async def set_valves(self, valves: Mapping[str, p.Valve]) -> None:
    """Turn syringe pumps' valves: the Master Controller's `VI`/`VO`/`VB`/`VE`. Each pump is sent Set Move Parameters at the position it is at, with
    its minimum start and cutoff velocities, maximum velocity and acceleration, and the new valve;
    then one Move All. Positions not known are read first, as before any move.

    Raises:
      ValueError: for an axis that is not a syringe pump, or an unknown valve.
    """
    pod = self._pod
    for name, valve in valves.items():
      if not pod.axis(name).is_pump:
        raise ValueError(f"{pod}: {name} is not a syringe pump")
      if valve not in p.VALVE_POSITIONS:
        raise ValueError(f"unknown valve {valve!r}; have {sorted(p.VALVE_POSITIONS)}")
    if not valves:
      return
    logger.info("%s V %s", pod.component, " ".join(f"{n}={v}" for n, v in valves.items()))
    try:
      await pod._read_unknown_positions(pumps=True)
      await self._unchecked_fw_set_valves(valves)
      await pod._unchecked_fw_move_all(VALVE_MOVE_ALL_TIMEOUT_MS)
    except BaseException:
      for name in valves:
        pod._valves[name] = None
      pod._forget_positions(valves)
      raise
    pod._valves.update(valves)

  async def _unchecked_fw_set_valves(self, valves: Mapping[str, p.Valve]) -> None:
    """Each pump's Set Move Parameters to turn its valve where it stands, sent together."""
    pod = self._pod
    requests = []
    for name, valve in valves.items():
      axis = pod.axis(name)
      limits, pump = axis.limits, axis.pump_limits
      requests.append(
        p.set_pump_move_parameters(
          axis.index,
          pod._cpositions[name][0] or 0,  # read before this is called
          pump.min_start_velocity,
          limits.max_velocity,
          pump.min_cutoff_velocity,
          limits.max_acceleration,
          valve,
        )
      )
    await pod.board.send_many(requests)

  # -- sensors -----------------------------------------------------------------------

  async def set_lls_sensitivity(self, value: int) -> None:
    """The liquid-level sensors' sensitivity, 0-4095: the Master Controller's `SS` (Biomek sends
    3500 on this instrument). Moves nothing.

    Raises:
      ValueError: outside 0..4095.
    """
    logger.info("%s SS %d", self._pod.component, value)
    await self._pod.board.send(p.set_lls_cd_sensitivity(False, value))

  async def request_tip_status(self) -> List[Optional[str]]:
    """Each probe's tip, `loaded`, `unloaded` or `unknown`, as the tip sensor reports it: the
    Master Controller's `GT`."""
    return p.parse_tip_status((await self._pod.board.send(p.get_tip_status())).payload)

  async def verify_tips(self, expected: Mapping[int, bool]) -> None:
    """Check that channels do or do not carry a tip, from the tip sensor: the Master Controller's
    `VT`. `expected` maps a probe (1..8) to whether it should have a tip.
    Moves nothing.

    Raises:
      TipCheckError: naming every probe that does not match, or whose tip cannot be determined.
    """
    states = await self.request_tip_status()
    for probe in expected:
      if not 1 <= probe <= len(states):
        raise ValueError(f"{self._pod}: no probe {probe}; have 1..{len(states)}")
    problems = tip_problems(states, expected)
    if problems:
      raise TipCheckError(f"{self._pod}: " + "\n".join(problems))

  # -- tips ---------------------------------------------------------------------------------------

  @property
  def tips(self) -> List[Optional[str]]:
    """Each probe's tip (its model), probe 1..8; None for none. Kept in the session state and
    checked against the tip sensor after each load and discard."""
    driver = self._pod.driver
    if driver is None:
      return [None] * PROBES
    tips = driver.state.pod(self._pod.side).tips
    return list(tips)[:PROBES] + [None] * (PROBES - len(tips))

  def _set_tips(self, probes: Mapping[int, Optional[str]]) -> None:
    driver = self._pod.driver
    if driver is None:
      return
    tips = self.tips
    for probe, model in probes.items():
      tips[probe - 1] = model
    driver.state.pod(self._pod.side).tips = tips
    if driver.state_json:
      driver.save_state()

  def _context(self) -> Tuple["BiomekI7Deck", "DeviceConfiguration", "PodConfiguration"]:
    driver, declared = self._pod.driver, self._pod.declared
    if driver is None or driver.deck is None or declared is None:
      raise RuntimeError(f"{self} needs a deck and a declared configuration (use `BiomekI7`)")
    return driver.deck, driver.declared, declared

  @staticmethod
  def tip_length(tip: Tip) -> float:
    """How far below its probe `tip`'s end is, cm: its length less its `SpanSeatingDepth`
    (`fitting_depth`)."""
    return float((tip.get_size_z() - tip.fitting_depth) / 10)

  def carried_tip_lengths(self) -> Dict[str, float]:
    """Per probe carrying a tip, its Z axis and how far below the probe the tip ends, cm: from the
    tips picked up this session, or from the tip class the session state names.

    Raises:
      RuntimeError: for a tip whose length is not known.
    """
    lengths = {}
    for probe, model in enumerate(self.tips, start=1):
      if model is None:
        continue
      tip = self._tips.get(probe)
      if tip is None and model.startswith("biomek_"):
        tip = biomek_tip(model[len("biomek_") :], "t")
      if tip is None:
        raise RuntimeError(f"{self}: the length of probe {probe}'s tip ({model}) is not known")
      lengths[f"Z{probe}"] = self.tip_length(tip)
    return lengths

  def _probe(self, channel: int) -> int:
    if not 0 <= channel < PROBES:
      raise ValueError(f"{self}: no channel {channel}; have 0..{PROBES - 1}")
    return channel + 1

  def plan_pick_up(
    self,
    tip_spots: Sequence[TipSpot],
    use_channels: Optional[Sequence[int]] = None,
    offsets: Optional[Sequence[Coordinate]] = None,
  ) -> List[PickUpPlan]:
    """The moves a pickup takes: one per set of spots the probes reach at once (one rack, one X,
    spaced evenly in Y for the probes taking them, a span the pod reaches), in channel order.

    Raises:
      ValueError: for a channel out of range or given twice, or not one channel per spot.
    """
    deck, configuration, declared = self._context()
    channels = list(range(len(tip_spots))) if use_channels is None else list(use_channels)
    if len(channels) != len(tip_spots) or len(set(channels)) != len(channels):
      raise ValueError(f"{self}: one distinct channel per tip spot, please")
    offsets = [Coordinate.zero()] * len(tip_spots) if offsets is None else list(offsets)
    number = self._pod.pod_number
    jobs: List[_Job] = []
    spots_of: Dict[str, Tuple[Tuple[TipSpot, float, float], ...]] = {}
    for channel, spot, offset in sorted(zip(channels, tip_spots, offsets), key=lambda j: j[0]):
      rack = spot.parent
      if rack is None:
        raise ValueError(f"{self}: tip spot {spot.name} is in no rack")
      at = targeting.target(deck, configuration, number, spot, offset=offset)
      top = targeting.target(deck, configuration, number, rack, z="t").z
      world_top = targeting.target(deck, configuration, 1, rack, z="t").z
      if rack.name not in spots_of:
        spots_of[rack.name] = tuple(
          (s, t.x, t.y)
          for s, t in (
            (s, targeting.target(deck, configuration, number, s)) for s in rack.get_all_items()
          )
        )
      jobs.append(_Job(self._probe(channel), spot, at.x, at.y, top, world_top, spots_of[rack.name]))
    low, high = declared.limits["Span"]
    plans: List[PickUpPlan] = []
    while jobs:
      first = jobs.pop(0)
      batch: List[_Job] = [first]
      span: Optional[float] = None
      for job in list(jobs):
        if abs(job.x - first.x) > ALIGNMENT_TOLERANCE or abs(job.top - first.top) > 1e-6:
          continue
        if span is None:
          wanted = (job.y - first.y) * (PROBES - 1) / (job.probe - first.probe)
          if not low - 1e-6 <= wanted <= high + 1e-6:
            continue
          span = wanted
        if abs(first.y + (job.probe - first.probe) * span / (PROBES - 1) - job.y) <= (
          ALIGNMENT_TOLERANCE
        ):
          batch.append(job)
          jobs.remove(job)
      plans.append(self._pick_up_plan(batch, low if span is None else span, declared))
    return plans

  def _pick_up_plan(
    self, batch: List["_Job"], span: float, declared: "PodConfiguration"
  ) -> PickUpPlan:
    s, geometry = declared.settings, declared.geometry
    first = batch[0]
    loading = s.get("LoadingOffsets") or [0.0] * PROBES
    probes = [job.probe for job in batch]
    y = first.y - (first.probe - 1) * span / (PROBES - 1)
    # Biomek also lowers every probe without a tip that stands over an emptied spot of the rack; one
    # over a spot still holding a tip, or over none, stays up (as logged). Spots whose contents are
    # not tracked count as holding tips.
    tips = self.tips

    def over_an_empty_spot(n: int) -> bool:
      yn = y + (n - 1) * span / (PROBES - 1)
      return any(
        abs(sx - first.x) <= ALIGNMENT_TOLERANCE
        and abs(sy - yn) <= ALIGNMENT_TOLERANCE
        and spot.tracks_tips
        and not spot.has_tip()
        for spot, sx, sy in first.rack_spots
      )

    over = [n for n in range(1, PROBES + 1) if tips[n - 1] is None and over_an_empty_spot(n)]
    return PickUpPlan(
      x=first.x,
      y=y,
      span=span,
      pairs=tuple((job.probe, job.spot) for job in batch),
      # Biomek's safe height over the rack, in its world model, which places labware by pod 1's
      # corner of the position (the press uses this pod's: they differ by up to 0.008 cm here).
      approach=first.world_top + geometry.additional_safe_height + geometry.avoidance_margin,
      lowered=tuple(sorted(set(probes) | set(over))),
      seats={n: first.top + s["TipLoadZOffset"] + loading[n - 1] for n in probes},
      exit=first.top + EXIT_ABOVE_TOP,
      groups=tuple(press_groups(probes, s["MaxLoadingProbes"], s["MinLoadingInterval"])),
    )

  def plan_discard(self, probes: Sequence[int], trash: Optional[Resource] = None) -> DiscardPlan:
    """Where a discard of `probes` (1..8) goes and how they are shucked, as Biomek does it.

    Raises:
      ValueError: for a position that is not a trash (no `tip_disposal`).
    """
    deck, configuration, declared = self._context()
    position = trash if trash is not None else deck.get_position(DEFAULT_TRASH)
    disposal = getattr(position, "tip_disposal", None)
    if disposal is None:
      raise ValueError(f"{position.name} is not a trash position (no tip disposal offsets)")
    corner = targeting.target(
      deck, configuration, self._pod.pod_number, position, x="l", y="b", z="b"
    )
    calibrated = configuration.deck_positions.get(position.name)
    x_span = calibrated.x_span if calibrated else position.get_size_x() / 10
    y_span = calibrated.y_span if calibrated else position.get_size_y() / 10
    s = declared.settings
    span = declared.limits["Span"][0]
    overshoot = s.get("DiscardOvershoot", 0.0)
    return DiscardPlan(
      x=corner.x + x_span / 2 + disposal.span8_x,
      y=corner.y + (y_span - span) / 2 + disposal.span8_y,
      span=span,
      z=s["DiscardHeight"] - DISCARD_BELOW,
      groups=tuple(press_groups(probes, s["MaxUnloadingProbes"], s["MinUnloadingInterval"])),
      shuck={n: declared.limits[f"Z{n}"][1] + overshoot for n in probes},
    )

  async def _travel(self, x: float, y: float, span: float, height: Optional[float]) -> None:
    """Probe 1 to X/Y with the span (Biomek cm), clear of the deck; `height` in mm (lowest points)."""
    _, _, declared = self._context()
    await self._pod.move_over(
      {
        "X": declared.to_counts("X", x),
        "Y": declared.to_counts("Y", y),
        "S": declared.to_counts("S", span),
      },
      minimum_traverse_height=None if height is None else height / 10,
    )

  async def pick_up_tips(
    self,
    tip_spots: Sequence[TipSpot],
    use_channels: Optional[List[int]] = None,
    offsets: Optional[List[Coordinate]] = None,
    minimum_traverse_height_start: Optional[float] = None,
    minimum_traverse_height_end: Optional[float] = None,
  ) -> None:
    """Pick up a tip from each spot, one channel per spot (STAR's `Pipettes` signature), as Biomek
    does: see the module's description. With `DetectTips` set, the tip sensor checks every probe
    afterwards.

    Args:
      tip_spots: where to pick up from, one per channel.
      use_channels: which channels, 0-indexed from the back (channel 0 is probe 1). The first
        `len(tip_spots)` when None.
      offsets: added to each spot's centre, in mm. None for none.
      minimum_traverse_height_start: how high the channels' lowest points (a tip's end, else the
        probe's) travel, mm above the deck. When None, the probes travel as high as they go.
      minimum_traverse_height_end: where the channels' lowest points are left, mm. When None, the
        probes are left as high as they go.

    Raises:
      HasTipError: if a channel already carries a tip.
      ValueError: for a channel out of range, or a height out of reach.
      TipCheckError: if the tip sensor disagrees afterwards.
    """
    if not tip_spots:
      return
    channels = list(range(len(tip_spots))) if use_channels is None else list(use_channels)
    tips = self.tips
    for channel in channels:
      if tips[self._probe(channel) - 1] is not None:
        raise HasTipError(f"{self}: channel {channel} already carries a tip")
    plans = self.plan_pick_up(tip_spots, channels, offsets)
    _, _, declared = self._context()
    pod = self._pod
    speed = round(declared.settings["LoadSpeed"] * 10)

    def z(probes: Sequence[int], height: float) -> Dict[str, int]:
      return {f"Z{n}": declared.to_counts(f"Z{n}", height) for n in probes}

    for plan in plans:
      probes = [n for n, _ in plan.pairs]
      logger.info(
        "%s pick up tips on probes %s at X %.3f Y %.3f S %.3f",
        pod.component, probes, plan.x, plan.y, plan.span,
      )  # fmt: skip
      await self._travel(plan.x, plan.y, plan.span, minimum_traverse_height_start)
      await pod.move_axes(z(plan.lowered, plan.approach))
      previous: Tuple[int, ...] = ()
      for group in plan.groups:
        targets = z(previous, plan.exit)
        targets.update({f"Z{n}": declared.to_counts(f"Z{n}", plan.seats[n]) for n in group})
        await pod.move_axes(targets, speed)
        previous = group
      await pod.move_axes(z(previous, plan.exit), speed)
      picked = {}
      for probe, spot in plan.pairs:
        tip = spot.tip_for_pickup()
        if spot.tracks_tips:
          spot.unassign_tip()
        self._tips[probe] = tip
        picked[probe] = tip.model
      self._set_tips(picked)
    end = minimum_traverse_height_end
    await pod.lift_channels(None if end is None else end / 10)
    await self._check_tips(sorted(n for plan in plans for n, _ in plan.pairs))

  async def drop_tips(self, tip_spots: Sequence[TipSpot], *args: Any, **kwargs: Any) -> None:
    """Not possible on a Span-8: its stripper pushes tips off only at the top of the probes'
    travel, so tips cannot be put back into a rack; they go to waste with `discard_tips`.

    Raises:
      NotImplementedError: always.
    """
    raise NotImplementedError(
      f"{self}: a Span-8 cannot put tips back into a rack (its stripper works only at the top of "
      "the probes' travel); use discard_tips"
    )

  async def discard_tips(
    self,
    use_channels: Optional[List[int]] = None,
    trash: Optional[Resource] = None,
    minimum_traverse_height_start: Optional[float] = None,
    minimum_traverse_height_end: Optional[float] = None,
  ) -> None:
    """Shuck the channels' tips into the trash (`TR2` by default), as Biomek does: see the module's
    description. With `DetectTips` set, the tip sensor checks every probe afterwards.

    Args:
      use_channels: which channels, 0-indexed from the back. Every channel carrying a tip when None.
      trash: the trash position.
      minimum_traverse_height_start: how high the channels' lowest points (the tips' ends) travel
        to the trash, mm above the deck. When None, the probes travel as high as they go.
      minimum_traverse_height_end: where the probes are left, mm. When None, as high as they go.

    Raises:
      RuntimeError: if no channel named carries a tip.
      TipCheckError: if the tip sensor disagrees afterwards.
    """
    tips = self.tips
    named = range(PROBES) if use_channels is None else use_channels
    probes = sorted(n for n in (self._probe(c) for c in named) if tips[n - 1] is not None)
    if not probes:
      raise RuntimeError(f"{self}: no channel named carries a tip")
    plan = self.plan_discard(probes, trash)
    _, _, declared = self._context()
    pod = self._pod
    logger.info("%s discard tips on probes %s", pod.component, probes)

    def z(group: Sequence[int], height: Mapping[int, float]) -> Dict[str, int]:
      return {f"Z{n}": declared.to_counts(f"Z{n}", height[n]) for n in group}

    below = {n: plan.z for n in probes}
    await self._travel(plan.x, plan.y, plan.span, minimum_traverse_height_start)
    await pod.move_axes(z(probes, below))
    speed = round(declared.settings["DiscardSpeed"] * 10)
    for group in plan.groups:
      await pod.move_axes(z(group, plan.shuck), speed)
      await pod.move_axes(z(group, below), speed)
    for probe in probes:
      self._tips.pop(probe, None)
    self._set_tips({n: None for n in probes})
    end = minimum_traverse_height_end
    await pod.lift_channels(None if end is None else end / 10)
    await self._check_tips(probes)

  async def _check_tips(self, probes: Sequence[int]) -> None:
    """With `DetectTips` set, the tip sensor checks the probes worked against the session state, as
    Biomek's `VT <probes> <expected>`."""
    declared = self._pod.declared
    if declared is not None and declared.settings.get("DetectTips"):
      tips = self.tips
      await self.verify_tips({n: tips[n - 1] is not None for n in probes})
