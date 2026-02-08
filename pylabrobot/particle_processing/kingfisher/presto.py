"""High-level KingFisher Presto machine frontend wrapping the backend.

Same lifecycle as other machines (setup(), stop(), async with).
Exposes start_protocol(), get_status(), acknowledge(), error_acknowledge(),
next_event() for the next (name, evt, ack), and events() for a raw event stream.
Main way to run: build or load a KingFisherProtocol and call run_protocol(protocol)
to upload and start; then drive the run by calling next_event() in a loop. Handle each
event (LoadPlate, RemovePlate, ChangePlate, Pause, etc.), do your work (robot, user),
then await ack() when required. Stop when name is Ready, Aborted, or Error.
"""

import warnings
import xml.etree.ElementTree as ET
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from pylabrobot.machines.machine import Machine, need_setup_finished

from .kingfisher_protocol import KingFisherProtocol
from .presto_backend import KingFisherPrestoBackend, TurntableLocation


class KingFisherPresto(Machine):
  """High-level KingFisher Presto magnetic particle processor.

  Wraps KingFisherPrestoBackend; same API pattern as Thermocycler/Machine.
  Use next_event() for (name, evt, ack) or events() for a raw event stream.
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
        msg += " You are attaching to an existing protocol run. To continue driving it, use next_event(attach=True) then next_event() in a loop; or call stop_protocol() or abort() to stop."
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
    Idle = no protocol in progress (ready for next command). Busy = protocol in progress (existing run); attach to continue with next_event() or stop with stop_protocol()/abort().
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
      message = "Protocol in progress (existing run). Attach to continue with next_event(), or stop_protocol() or abort() to stop."
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
  async def run_protocol(
    self,
    protocol: KingFisherProtocol,
    tip_name: Optional[str] = None,
    step_name: Optional[str] = None,
  ) -> None:
    """Upload the protocol and start it. Caller must drive the run with next_event() in a loop.

    Build or load a KingFisherProtocol, then call run_protocol(protocol) to run the full
    protocol, or run_protocol(protocol, step_name=\"Mix1\") to run from a specific step.
    For single-tip protocols, when step_name is set and tip_name is None, tip_name defaults
    to protocol.tips[0].name (protocol must have at least one tip).
    Then loop: name, evt, ack = await next_event(); handle event (e.g. load/remove plate);
    if ack: await ack(); stop when name is Ready, Aborted, or Error.
    """
    if step_name is not None and tip_name is None and len(protocol.tips) == 1:
      tip_name = protocol.tips[0].name
    if step_name is not None and tip_name is None and len(protocol.tips) == 0:
      raise ValueError("Protocol has no tips; specify tip_name when running from a step.")
    await self.upload_protocol(protocol.name, protocol.to_bdz())
    await self.start_protocol(protocol.name, tip=tip_name, step=step_name)

  @need_setup_finished
  async def stop_protocol(self) -> None:
    """Stop ongoing protocol/step execution."""
    return await self.backend.stop_protocol()

  @need_setup_finished
  async def next_event(
    self, *, attach: bool = False
  ) -> Tuple[str, Optional[ET.Element], Optional[Callable[..., Any]]]:
    """Wait for the next event; returns (name, evt, ack). Single API for \"get next event\".

    Blocks until the instrument sends the next event (no polling). Returns (name, evt, ack)
    where evt is the raw XML element. Call when ready for the next event; do other work
    (robot moves, user prompts, other instruments) between calls. For LoadPlate, RemovePlate,
    ChangePlate, Pause call await ack() when ready so the run continues. Stop when name is
    Ready, Aborted, or Error. Raw event stream without interpretation: use events().

    When attach=True (e.g. attaching to an in-progress run after setup when Busy), if status
    is already Idle or In error, returns (\"Ready\", None, None) without reading from the queue.
    """
    if attach:
      status_dict = await self.get_status()
      status = status_dict.get("status") or "In error"
      if status in ("Idle", "In error"):
        return ("Ready", None, None)
    evt = await self.backend.get_event()
    name = evt.get("name")
    if name in ("LoadPlate", "RemovePlate", "ChangePlate", "Pause"):
      return (name, evt, self.acknowledge)
    if name == "Error":
      return (name, evt, self.error_acknowledge)
    if name in ("Ready", "Aborted"):
      return (name, evt, None)
    # Informational (StepStarted, ProtocolTimeLeft, Temperature, ChangeMagnets): no ack
    return (name, evt, None)

  def events(self):
    """Async generator of raw events (ET.Element). Use for low-level control; for (name, evt, ack) use next_event()."""
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
