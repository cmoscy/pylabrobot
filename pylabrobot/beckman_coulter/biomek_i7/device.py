"""The Biomek i7 as one resource: its frame, its deck, and its two bridges with their pods.

The frame's proportions, in centimetres:

- `BaseXSpan` 188.0 on an i7 (130.0 on the single-pod frame), 91.025 deep, from 17 left of Biomek's
  X origin and 11 behind its Y origin.
- `BaseHeight` 46.355: the cabinet under the deck plate.
- Three 2.667 plates stacked under the deck surface.
- Corner posts 8 x 8 standing `BaseHeight` + 20 - 8 = 58.355 above the plates, carrying the rails the
  bridges ride on, 8 x 8 in section.

The frame here is the device resource's own: origin at the front left bottom corner of the cabinet, millimetres, Y towards the back.
"""

import logging
from typing import Callable, Dict, Iterable, List, Optional, Set, Tuple

from pylabrobot.beckman_coulter.biomek_i7.deck import (
  FRAME_X,
  FRAME_Y,
  BiomekI7Deck,
  biomek_x,
  biomek_y,
  deck_x,
  deck_y,
)
from pylabrobot.beckman_coulter.biomek_i7.driver.features.chassis import DoorState
from pylabrobot.beckman_coulter.biomek_i7.driver.features.gripper import Gripper
from pylabrobot.beckman_coulter.biomek_i7.driver.features.head96 import (
  DEFAULT_TRASH as HEAD96_TRASH,
)
from pylabrobot.beckman_coulter.biomek_i7.driver.features.head96 import Head96
from pylabrobot.beckman_coulter.biomek_i7.driver.features.pipettes import (
  DEFAULT_TRASH as SPAN8_TRASH,
)
from pylabrobot.beckman_coulter.biomek_i7.driver.features.pipettes import Pipettes
from pylabrobot.beckman_coulter.biomek_i7.driver.features.pod import Pod
from pylabrobot.beckman_coulter.biomek_i7.driver.master import BiomekI7Driver
from pylabrobot.beckman_coulter.biomek_i7.driver.motors import MotorTables
from pylabrobot.beckman_coulter.biomek_i7.driver.simulator import (
  sense_recorded_tips,
  simulated_boards,
)
from pylabrobot.beckman_coulter.biomek_i7.driver import pod_geometry as g
from pylabrobot.beckman_coulter.biomek_i7.driver.configuration import PodGeometry
from pylabrobot.beckman_coulter.biomek_i7.resource_model import cm, pod_model
from pylabrobot.beckman_coulter.biomek_i7.resource_model.pods import HEAD_FLANGE_HEIGHT, HEAD_HEIGHT
from pylabrobot.resources.beckman_coulter.tips import beckman_coulter_biomek_tip_1070uL
from pylabrobot.resources.coordinate import Coordinate
from pylabrobot.resources.resource import Resource

logger = logging.getLogger(__name__)

BASE_X_SPAN = cm(188.0)  # BaseXSpan default on an i7
BASE_Y_SPAN = cm(91.025)
BASE_LEFT_OF_X0 = cm(17.0)  # the base starts 17 cm left of Biomek's X origin
BASE_BEHIND_Y0 = cm(11.0)  # and 11 cm behind its Y origin
CABINET_HEIGHT = cm(46.355)  # BaseHeight
PLATE_THICKNESS = cm(2.667)
PLATES = 3
POST_SECTION = cm(8.0)
POST_HEIGHT = cm(46.355 + 20.0 - 8.0)
RAIL_SECTION = cm(8.0)

DECK_Z = CABINET_HEIGHT + PLATES * PLATE_THICKNESS
# The height the bridges ride at. The posts and rails are not drawn, so the device's own extent ends
# at the deck surface and the bridges and pods stand above it.
RAIL_TOP_Z = DECK_Z + POST_HEIGHT + RAIL_SECTION
SIZE_Z = DECK_Z

# Where the deck's frame sits in the device's. Biomek X = deck x - FRAME_X, and the base begins
# BASE_LEFT_OF_X0 left of Biomek X 0; Biomek Y = FRAME_Y - deck y, and the base's back edge is
# BASE_BEHIND_Y0 behind Biomek Y 0.
DECK_X = BASE_LEFT_OF_X0 - FRAME_X
DECK_Y = BASE_Y_SPAN - BASE_BEHIND_Y0 - FRAME_Y


# The pods a model-only device shows, as on the instrument this was written for.
MODEL_MODULES = {"left": "multichannel", "right": "span8"}


class BiomekI7(Resource):
  """A Biomek i7: its frame, a deck, and a bridge per pod.

  Each pod's model (`pod_models[side]`) rides its bridge (`bridges[side]`). With a driver, the
  models are built from the instrument's declared geometry once it is set up, and each bridge
  follows its pod's X as the driver moves it, placed as Biomek's collision model places it, so
  the resource tree takes up the X the rail guard counts. The Multichannel's model also follows its
  Y and Z (`_show_pod`). `move_bridge` and `move_pod` place the models by hand.
  """

  def __init__(
    self,
    name: str = "biomek_i7",
    deck: Optional[BiomekI7Deck] = None,
    driver: Optional[BiomekI7Driver] = None,
    simulation: bool = False,
    configuration_json: Optional[str] = None,
    simulator_recording: Optional[str] = None,
    state_json: Optional[str] = None,
    simulate_motion_time: bool = False,
    motion_time_scale: float = 0.25,
  ):
    """
    Args:
      name: the resource's name.
      deck: the deck. Defaults to an empty one (place ALPs with `deck.assign_alp`).
      driver: what talks to the instrument.
      simulation: drive a simulated instrument instead (`driver/simulator.py`): powered up, not
        configured, not homed. Ignored when a driver is given.
      configuration_json: the instrument's declared configuration (its calibration, axis scales
        and limits, deck positions), for the driver this builds; `$BIOMEK_I7_CONFIGURATION` when
        None. Ignored when a driver is given.
      simulator_recording: the cold-start recording a simulated instrument answers identity and
        sensor reads from; `$BIOMEK_I7_SIMULATOR_RECORDING` when None.
      state_json: where the driver this builds keeps the tips on each head between sessions
        (`BiomekI7Driver`'s `state_json`).
      simulate_motion_time: whether a simulated move takes time at all, as the STAR's and the
        Prep's simulators take it: the time the instrument's drives would take (from each move's
        distance, speed and acceleration), so a viewer shows each step. Off, every move answers at
        once.
      motion_time_scale: the share of the instrument's time a move takes when it does: a quarter,
        so a step is watched rather than waited for. 1.0 keeps the instrument's time.

    With neither a driver nor `simulation`, the device is a model only, for the visualizer and
    planning, and `setup` raises. (The STAR refuses that; here the model is useful on its own.)
    """
    if driver is not None and simulation:
      logger.warning("both a driver and simulation given; driving the driver")
    if driver is None and simulation:
      tables = MotorTables.load(configuration_json)
      boards = simulated_boards(
        motor_tables=tables,
        path=simulator_recording,
        time_scale=motion_time_scale if simulate_motion_time else 0.0,
      )
      driver = BiomekI7Driver(
        declared_configuration_json=configuration_json, boards=boards, state_json=state_json
      )
      sense_recorded_tips(driver, boards)  # the simulator does not model tips
    elif driver is None and configuration_json is not None:
      driver = BiomekI7Driver(declared_configuration_json=configuration_json, state_json=state_json)
    self.driver = driver
    super().__init__(
      name=name,
      size_x=BASE_X_SPAN,
      size_y=BASE_Y_SPAN,
      size_z=SIZE_Z,
      category="device",
      model="beckman_coulter_biomek_i7",
    )
    self.deck = deck if deck is not None else BiomekI7Deck()
    if driver is not None:
      driver.deck = self.deck  # the operations work on this deck (STAR's `driver.deck`)
    self.assign_child_resource(self.deck, location=Coordinate(DECK_X, DECK_Y, DECK_Z))

    self.bridges: Dict[str, Resource] = {}
    self.pod_models: Dict[str, Resource] = {}
    self._unfollow: List[Callable[[], None]] = []
    for side, module_type in MODEL_MODULES.items():
      self._build_pod_model(side, module_type)
    if driver is not None and driver.pods:
      self._follow_driver()  # given a driver already set up

  # -- session ---------------------------------------------------------------

  def _require_driver(self) -> BiomekI7Driver:
    if self.driver is None:
      raise RuntimeError(
        f"{self.name} has no driver; pass `driver=BiomekI7Driver()` or `simulation=True`"
      )
    return self.driver

  # -- what the instrument has (read through to the driver) ------------------------
  # `left_pod` / `right_pod` are the driver's pods (STAR's `left_x_arm` / `right_x_arm`);
  # `pod_models` and `bridges` are their models in the resource tree.

  @property
  def left_pod(self) -> Optional[Pod]:
    """The left pod, on an instrument that has one (known after `setup`)."""
    return self._require_driver().left_pod

  @property
  def right_pod(self) -> Optional[Pod]:
    """The right pod, on an instrument that has one (known after `setup`)."""
    return self._require_driver().right_pod

  @property
  def pods(self) -> List[Pod]:
    """The pods, left first."""
    return self._require_driver().pods

  @property
  def head96(self) -> Optional[Head96]:
    """The 96-channel head, on an instrument where one pod has it."""
    return self._require_driver().head96

  @property
  def pipettes(self) -> Optional[Pipettes]:
    """The Span-8 channels, on an instrument where one pod has them."""
    return self._require_driver().pipettes

  @property
  def gripper(self) -> Optional[Gripper]:
    """The gripper, on an instrument with one pod; with two, reach it as `left_pod.gripper` or
    `right_pod.gripper`."""
    return self._require_driver().gripper

  async def setup(
    self,
    skip_device_initialization: bool = False,
    skip_left_pod: bool = False,
    skip_right_pod: bool = False,
    force_initialize: bool = False,
    discard_tips: bool = True,
    park: bool = True,
  ) -> None:
    """Bring the instrument up. This homes the arms, then, as the STAR does, takes off any tips
    left on (`discard_tips`), and parks the pods, as Biomek's Home All does (`park`).

    Args:
      skip_device_initialization: as `BiomekI7Driver.setup` takes it.
      skip_left_pod: as `BiomekI7Driver.setup` takes it.
      skip_right_pod: as `BiomekI7Driver.setup` takes it.
      force_initialize: as `BiomekI7Driver.setup` takes it.
      discard_tips: shuck tips found on a pod that was brought up into its trash: on a Span-8 the
        probes the tip sensor finds a tip on, on the 96-head the tips the session state records
        (it has no tip sensor). Needs the trash position on the deck.
      park: park every pod that was brought up (`park`).
    """
    await self._require_driver().setup(
      skip_device_initialization=skip_device_initialization,
      skip_left_pod=skip_left_pod,
      skip_right_pod=skip_right_pod,
      force_initialize=force_initialize,
    )
    self._follow_driver()
    skipped = {side for side, skip in (("left", skip_left_pod), ("right", skip_right_pod)) if skip}
    if discard_tips:
      await self._discard_found_tips(skipped)
    if park:
      await self.park(skip=skipped)

  async def _discard_found_tips(self, skipped: Set[str]) -> None:
    """Shuck the tips found on the pods not `skipped` (`setup`'s `discard_tips`). A Span-8 probe
    the sensor finds a tip on that the session did not record is taken to carry the longest
    Biomek tip, so its travel to the trash clears the deck; a recorded tip the sensor does not
    find is forgotten."""
    names = set(self.deck.position_names)
    for pod in self.pods:
      if pod.side in skipped:
        continue
      if pod.pipettes is not None and SPAN8_TRASH in names:
        pipettes = pod.pipettes
        states = await pipettes.request_tip_status()
        found = [n for n, state in enumerate(states, start=1) if state == "loaded"]
        gone = [n for n, t in enumerate(pipettes.tips, start=1) if t is not None and n not in found]
        if gone:
          pipettes._set_tips({n: None for n in gone})
        if found:
          assumed = beckman_coulter_biomek_tip_1070uL("assumed tip")
          pipettes._set_tips({n: pipettes._carried_tip(n) or assumed for n in found})
          logger.warning("%s: tips found on probes %s; discarding them", pod, found)
          await pipettes.discard_tips(use_channels=[n - 1 for n in found])
      if pod.head96 is not None and HEAD96_TRASH in names and any(pod.head96.tips):
        logger.warning("%s: the 96-head carries tips, as recorded; discarding them", pod)
        await pod.head96.discard_tips()

  async def park(self, skip: Iterable[str] = ()) -> None:
    """Park the pods as Biomek does (`Pod.park`; a Span-8 over its wash station,
    `pipettes.park`), the left pod first, as each pod's park is at its own end of the rail. `skip`:
    sides to leave as they are."""
    for pod in sorted(self._require_driver().pods, key=lambda p: p.side != "left"):
      if pod.side in skip:
        continue
      if pod.pipettes is not None:
        await pod.pipettes.park()
      else:
        await pod.park()

  async def home_all(self, force: bool = True) -> None:
    """Biomek's Home All: bring the instrument up again, by default homing every axis as Biomek
    does. `force=False` homes only what is not homed."""
    await self._require_driver().initialize(force_initialize=force)

  async def request_door_state(self) -> DoorState:
    """Whether the door is open or closed. Moves nothing."""
    driver = self._require_driver()
    if driver.chassis is None:
      raise RuntimeError("no chassis; call `setup()` first")
    return await driver.chassis.request_door_state()

  async def stop(self) -> None:
    """Release the instrument. Moves nothing."""
    for unfollow in self._unfollow:
      unfollow()
    self._unfollow = []
    await self._require_driver().stop()

  # -- the pods' models ------------------------------------------------------------

  def _build_pod_model(
    self, side: str, module_type: str, geometry: Optional[PodGeometry] = None
  ) -> None:
    """(Re)build a side's bridge and pod, keeping the bridge where it was."""
    old = self.bridges.pop(side, None)
    rail, pod = pod_model(side, module_type, BASE_Y_SPAN, geometry)
    if old is not None and old.location is not None:
      x = old.location.x
      self.unassign_child_resource(old)
    elif side == "left":
      x = POST_SECTION
    else:
      x = BASE_X_SPAN - POST_SECTION - rail.get_size_x()
    # The bridges ride on the rails' top faces.
    self.assign_child_resource(rail, location=Coordinate(x, 0, RAIL_TOP_Z))
    self.bridges[side], self.pod_models[side] = rail, pod

  def _follow_driver(self) -> None:
    """Rebuild each pod's model from the driver's pod (its module type and declared geometry) and
    follow its X from now on."""
    for unfollow in self._unfollow:
      unfollow()
    self._unfollow = []
    for pod in self._require_driver().pods:
      geometry = pod.declared.geometry if pod.declared is not None else None
      self._build_pod_model(pod.side, pod.module_type, geometry)
      if pod.declared is not None:
        self._unfollow.append(pod.add_position_listener(self._show_pod))
        self._show_pod(pod)

  def _show_pod(self, pod: Pod) -> None:
    """Put a pod's model where the pod's axes are (an unknown one: where it was).

    - X: the bridge's left edge at `bridge_x_range` in Biomek's X.
    - Y and Z, on a Multichannel: Biomek's Y and Z name mandrel A1 (checked against the operations
      Biomek logged: over a plate's well A1, and Z the bare mandrel's bottom above this deck's
      surface). The carriage's back edge is `Mandrel1YOffset` behind it, and the head's flange
      bottom is taken as the mandrels' bottom.
    The Span-8's Y and Z channels are not followed: no logged operation checks them.
    """
    d = pod.declared
    if d is None:
      return
    x = pod.cpositions["X"][0]
    if x is not None:
      low, _ = g.bridge_x_range(d.geometry, d.to_value("X", x))
      self.move_bridge(pod.side, deck_x(cm(low)))
    if pod.module_type != "multichannel":
      return
    y, z = pod.cpositions["Y"][0], pod.cpositions["Z"][0]
    if y is None or z is None:
      return
    front = d.to_value("Y", y) - d.geometry.mandrel1_y_offset + d.geometry.pod_y_span
    self.move_pod(
      pod.side, deck_y(cm(front)), z=cm(d.to_value("Z", z) + HEAD_HEIGHT + HEAD_FLANGE_HEIGHT)
    )

  def traverse_clearance(
    self, side: str, x: Tuple[float, float], y: Tuple[float, float], tip_length: float = 0.0
  ) -> float:
    """The lowest height (Biomek Z, cm) a pod's channels may travel at over the area `x` by `y`
    (Biomek X/Y, cm), for `Pod.move_over`'s `clearance`: the top of the tallest resource on the
    deck there, plus the pod's `AdditionalSafeHeight`, plus `tip_length` (cm the tips reach below
    the channels' bottom). Biomek's Z is the deck model's z in cm (design.md B5). The deck itself,
    its ALPs' frames included, counts; the pods' own models do not."""
    pod = self._require_driver().pod(side)
    if pod.declared is None:
      raise RuntimeError(f"{pod} has no declared configuration")
    top = 0.0
    for resource in self.deck.get_all_children():
      loc = resource.get_location_wrt(self.deck)
      left, right = biomek_x(loc.x) / 10, biomek_x(loc.x + resource.get_size_x()) / 10
      back, front = biomek_y(loc.y + resource.get_size_y()) / 10, biomek_y(loc.y) / 10
      if left < x[1] and x[0] < right and back < y[1] and y[0] < front:
        top = max(top, (loc.z + resource.get_size_z()) / 10)
    return top + pod.declared.geometry.additional_safe_height + tip_length

  def move_bridge(self, side: str, x: float) -> None:
    """Put a side's bridge's left edge at deck x `x` mm, carrying its pod along the rails."""
    rail = self.bridges[side]
    loc = rail.location
    if loc is None:
      raise ValueError(f"{rail.name} is not placed on {self.name}")
    rail.location = Coordinate(DECK_X + x, loc.y, loc.z)

  def move_pod(self, side: str, y: float, z: Optional[float] = None) -> None:
    """Put a side's pod's front edge at deck y `y` mm along its bridge, and its carriage bottom at
    deck z."""
    pod = self.pod_models[side]
    loc = pod.location
    if loc is None or pod.parent is None:
      raise ValueError(f"{pod.name} is not on a bridge")
    new_z = loc.z if z is None else z + DECK_Z - RAIL_TOP_Z
    pod.location = Coordinate(loc.x, DECK_Y + y, new_z)
