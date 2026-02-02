"""High-level KingFisher Presto machine frontend wrapping the backend.

Same lifecycle as other machines (setup(), stop(), async with).
Exposes start_protocol(), get_status(), acknowledge(), error_acknowledge(),
and async for evt in backend.events() for event-based orchestration.
Liquid-handler-like API: mix(), dry(), collect_beads(), release_beads(), pause().
"""

import warnings
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional, Tuple, Union

from pylabrobot.machines.machine import Machine, need_setup_finished

from .bdz_builder import (
  STEP_SLOTS,
  build_collect_beads_bdz,
  build_dry_bdz,
  build_mix_bdz,
  build_pause_bdz,
  build_release_beads_bdz,
)
from .presto_backend import KingFisherPrestoBackend, TurntableLocation


class KingFisherPresto(Machine):
  """High-level KingFisher Presto magnetic particle processor.

  Wraps KingFisherPrestoBackend; same API pattern as Thermocycler/Machine.
  Use async for evt in self.backend.events() to react to LoadPlate, RemovePlate, etc.
  """

  def __init__(self, backend: KingFisherPrestoBackend):
    super().__init__(backend=backend)
    self.backend: KingFisherPrestoBackend = backend
    self._last_run_state: Optional[Dict[str, Any]] = None

  async def setup(self, *, initialize_turntable: bool = False, **backend_kwargs) -> None:
    """Connect (backend.setup), then check run state; warn if instrument is not Idle (Busy or In error).

    When initialize_turntable is True, the backend rotates to power-on state on connect so
    turntable positions are known; the table may move. Passed to backend.setup().
    """
    await super().setup(initialize_turntable=initialize_turntable, **backend_kwargs)
    state = await self.get_run_state()
    if state.get("status") != "Idle":
      msg = state.get("message", "")
      if state.get("status") == "Busy":
        msg += " To attach to the run and handle events, call presto.continue_run() and iterate the returned generator."
      elif state.get("status") == "In error":
        msg += " Check error_code and error_text; you may need to call error_acknowledge()."
      warnings.warn(msg, stacklevel=2)

  @need_setup_finished
  async def get_status(self) -> dict:
    """Get instrument status: Idle, Busy, or In error. Returns dict with ok, status, error_code, error_text, error_code_description."""
    return await self.backend.get_status()

  @need_setup_finished
  async def get_protocol_time_left(self, protocol: Optional[str] = None) -> Dict[str, Optional[str]]:
    """Get time left of protocol or single-step execution. Returns time_left and time_to_pause (XML Duration, e.g. PT2M42S).
    For single-step execution time_to_pause is None per spec."""
    return await self.backend.get_protocol_time_left(protocol)

  @need_setup_finished
  async def get_run_state(self) -> Dict[str, Any]:
    """Return instrument run state: get_status() keys plus time_left, time_to_pause, and message.
    Idle = no protocol in progress (ready for next command). Busy = protocol in progress; call continue_run() to attach.
    Does not warn; setup() calls this and warns when status is not Idle."""
    status_dict = await self.backend.get_status()
    status = status_dict.get("status") or "In error"
    time_left: Optional[str] = None
    time_to_pause: Optional[str] = None
    if status == "Busy":
      try:
        t = await self.backend.get_protocol_time_left()
        time_left = t.get("time_left")
        time_to_pause = t.get("time_to_pause")
      except Exception:
        pass
    if status == "Idle":
      message = "No protocol in progress (ready for next command)."
    elif status == "Busy":
      message = "Protocol in progress. Call continue_run() to attach to the event stream."
      if time_left is not None:
        message += f" Time left: {time_left}."
    else:
      message = "Instrument in error. You may need to call error_acknowledge(); see error_code and error_text."
    result: Dict[str, Any] = {
      **status_dict,
      "time_left": time_left,
      "time_to_pause": time_to_pause,
      "message": message,
    }
    self._last_run_state = result
    return result

  @need_setup_finished
  async def list_protocols(self) -> Tuple[List[str], int]:
    """List protocols in instrument memory. Returns (protocol_names, memory_used_percent)."""
    return await self.backend.list_protocols()

  @need_setup_finished
  async def download_protocol(self, name: str) -> bytes:
    """Download a protocol from instrument memory. Returns raw protocol bytes."""
    return await self.backend.download_protocol(name)

  @need_setup_finished
  async def upload_protocol(self, name: str, protocol_bytes: bytes, crc: Optional[int] = None) -> None:
    """Upload a protocol to instrument memory. crc optional (default: computed from bytes)."""
    return await self.backend.upload_protocol(name, protocol_bytes, crc=crc)

  @need_setup_finished
  async def start_protocol(
    self,
    protocol: str,
    tip: Optional[str] = None,
    step: Optional[str] = None,
  ) -> None:
    """Start a protocol or single step. Protocol must already be in instrument memory."""
    return await self.backend.start_protocol(protocol, tip=tip, step=step)

  @need_setup_finished
  async def stop_protocol(self) -> None:
    """Stop ongoing protocol/step execution."""
    return await self.backend.stop_protocol()

  @need_setup_finished
  async def get_event(self) -> ET.Element:
    """Return the next event from the queue. Blocks until one is available."""
    return await self.backend.get_event()

  def events(self):
    """Async generator of events. Use: async for evt in self.events() to orchestrate with other instruments."""
    return self.backend.events()

  @need_setup_finished
  async def acknowledge(self) -> None:
    """Send Acknowledge (e.g. after LoadPlate, RemovePlate, ChangePlate, Pause)."""
    return await self.backend.acknowledge()

  @need_setup_finished
  async def error_acknowledge(self) -> None:
    """Send ErrorAcknowledge to clear instrument error state."""
    return await self.backend.error_acknowledge()

  @need_setup_finished
  async def abort(self) -> None:
    """Two-phase abort: stops execution and flushes communication buffers."""
    return await self.backend.abort()

  async def _event_stream_until_ready(self, attach: bool):
    """Internal async generator of (name, evt, ack_callback) until Ready, Aborted, or Error.
    When attach=True: if status is Idle or In error, yield (Ready, None, None) and return; if Busy, enter event loop.
    When attach=False: go straight into event loop. Never calls stop_protocol() on exit."""
    if attach:
      status_dict = await self.get_status()
      status = status_dict.get("status") or "In error"
      if status in ("Idle", "In error"):
        yield ("Ready", None, None)
        return
    async for evt in self.events():
      name = evt.get("name")
      if name in ("LoadPlate", "RemovePlate", "ChangePlate", "Pause"):
        yield (name, evt, self.acknowledge)
      elif name == "Error":
        yield (name, evt, self.error_acknowledge)
        return
      elif name in ("Ready", "Aborted"):
        yield (name, evt, None)
        return
      # Informational (StepStarted, ProtocolTimeLeft, Temperature, ChangeMagnets): pass through, no ack
      else:
        yield (name, evt, None)

  def run_until_ready(self):
    """Async generator of (name, evt, ack_callback) until Ready, Aborted, or Error.
    Use after start_protocol() or mix()/dry()/etc. to drain events. Never stops the protocol on exit;
    call stop_protocol() or abort() to stop. Ready (event) and Idle (get_status) both mean run complete per spec."""
    return self._event_stream_until_ready(attach=False)

  def continue_run(self):
    """Async generator to attach to an in-progress run (same (name, evt, ack) as run_until_ready()).
    Use after setup() when get_run_state() or the setup warning indicated Busy. Call acknowledge() or
    error_acknowledge() when you choose; no automatic unstick."""
    return self._event_stream_until_ready(attach=True)

  async def _run_until_ready(self) -> None:
    """Drain run_until_ready() and auto-ack; used by mix(), dry(), etc. when wait_until_ready=True."""
    async for name, evt, ack in self.run_until_ready():
      if ack is not None:
        await ack()

  @need_setup_finished
  async def mix(
    self,
    plate: str,
    duration_sec: float,
    speed: str = "Medium",
    *,
    image: str = "Wash",
    loop_count: int = 3,
    wait_until_ready: bool = True,
  ) -> None:
    """Run a single Mix step (build .bdz, upload to plr_Mix, start, optionally wait until Ready).

    Supported speeds: Medium, Fast. Unsupported: Slow, Bottom mix, Half mix.
    """
    slot = STEP_SLOTS["Mix"]
    bdz_bytes = build_mix_bdz(slot, plate, duration_sec, speed, image=image, loop_count=loop_count)
    await self.upload_protocol(slot, bdz_bytes)
    await self.start_protocol(slot)
    if wait_until_ready:
      await self._run_until_ready()

  @need_setup_finished
  async def dry(
    self,
    duration_sec: float,
    *,
    plate: str = "Plate1",
    tip_position: str = "AboveSurface",
    wait_until_ready: bool = True,
  ) -> None:
    """Run a single Dry step (build .bdz, upload to plr_Dry, start, optionally wait until Ready)."""
    slot = STEP_SLOTS["Dry"]
    bdz_bytes = build_dry_bdz(slot, duration_sec, plate_name=plate, tip_position=tip_position)
    await self.upload_protocol(slot, bdz_bytes)
    await self.start_protocol(slot)
    if wait_until_ready:
      await self._run_until_ready()

  @need_setup_finished
  async def collect_beads(
    self,
    *,
    count: int = 3,
    collect_time_sec: float = 30,
    plate: str = "Plate1",
    wait_until_ready: bool = True,
  ) -> None:
    """Run a single CollectBeads step (build .bdz, upload to plr_CollectBeads, start, optionally wait until Ready). Count 1..5."""
    slot = STEP_SLOTS["CollectBeads"]
    bdz_bytes = build_collect_beads_bdz(slot, count, collect_time_sec, plate_name=plate)
    await self.upload_protocol(slot, bdz_bytes)
    await self.start_protocol(slot)
    if wait_until_ready:
      await self._run_until_ready()

  @need_setup_finished
  async def release_beads(
    self,
    duration_sec: float,
    *,
    speed: str = "Fast",
    plate: str = "Plate1",
    wait_until_ready: bool = True,
  ) -> None:
    """Run a single ReleaseBeads step (build .bdz, upload to plr_ReleaseBeads, start, optionally wait until Ready)."""
    slot = STEP_SLOTS["ReleaseBeads"]
    bdz_bytes = build_release_beads_bdz(slot, duration_sec, speed, plate_name=plate)
    await self.upload_protocol(slot, bdz_bytes)
    await self.start_protocol(slot)
    if wait_until_ready:
      await self._run_until_ready()

  @need_setup_finished
  async def pause(
    self,
    *,
    message: str = "",
    wait_until_ready: bool = True,
  ) -> None:
    """Run a single Pause step (build .bdz, upload to plr_Pause, start, optionally wait until Ready)."""
    slot = STEP_SLOTS["Pause"]
    bdz_bytes = build_pause_bdz(slot, message)
    await self.upload_protocol(slot, bdz_bytes)
    await self.start_protocol(slot)
    if wait_until_ready:
      await self._run_until_ready()

  @need_setup_finished
  async def rotate(
    self,
    position: int = 1,
    location: Union[str, TurntableLocation] = TurntableLocation.LOADING,
  ) -> None:
    """Rotate the turntable so the given position (1 or 2) is at the given location.

    The turntable has two positions (slots) 1 and 2. Each can be at \"processing\" (under the
    magnetic head) or \"loading\" (the load/unload station). The backend waits for Ready/Error.
    For explicit state use get_turntable_state(); for bringing the plate at loading to
    processing use load_plate().
    """
    await self.backend.rotate(position=position, location=location)

  @need_setup_finished
  async def get_turntable_state(self) -> Dict[int, Optional[str]]:
    """Return current location of each position: {1: 'processing'|'loading'|None, 2: ...}.

    State is inferred only from rotate() commands that completed with Ready; unknown after
    setup/stop until the first successful rotate (or setup(initialize_turntable=True)).
    """
    return self.backend.get_turntable_state()

  @need_setup_finished
  async def load_plate(self) -> None:
    """Rotate the table so whatever is at the loading position moves to the processing position.

    Requires known turntable state; call rotate() first or setup(initialize_turntable=True)
    if state is unknown. For explicit control use rotate() and get_turntable_state().
    """
    await self.backend.load_plate()

  @need_setup_finished
  async def pick_up_tips(self) -> None:
    """Run a single PickUpTips step (build .bdz, upload, start, wait for Ready).

    Not yet implemented; requires PickUpTips step XML and build_pick_up_tips_bdz() in bdz_builder.
    Until then use start_protocol(protocol, tip=..., step=...) with a protocol that has a tip pickup step.
    """
    await self.backend.pick_up_tips()

  @need_setup_finished
  async def drop_tips(self) -> None:
    """Run a single DropTips step (build .bdz, upload, start, wait for Ready).

    Not yet implemented; requires DropTips step XML and build_drop_tips_bdz() in bdz_builder.
    Until then use start_protocol(protocol, tip=..., step=...) with a protocol that has a drop-tips step.
    """
    await self.backend.drop_tips()

  @property
  def instrument(self) -> Optional[str]:
    """Instrument name/type from Connect response."""
    return self.backend.instrument

  @property
  def version(self) -> Optional[str]:
    """Firmware version from Connect response."""
    return self.backend.version

  @property
  def serial(self) -> Optional[str]:
    """Instrument serial number from Connect response."""
    return self.backend.serial
