"""
KingFisher Presto backend: MachineBackend wrapping the connection layer.

Exposes connect, disconnect, get_status, list_protocols, download/upload_protocol,
acknowledge, error_acknowledge, abort, and event queue/callback.
See KingFisher Presto Interface Specification for Cmd/Res/Evt formats and error codes.
"""

import asyncio
import base64
import binascii
import logging
import xml.etree.ElementTree as ET
from typing import Callable, Dict, List, Optional, Tuple

from pylabrobot.machines.backend import MachineBackend

from .error_codes import get_error_code_description
from .presto_connection import (
  KINGFISHER_PID,
  KINGFISHER_VID,
  PrestoConnection,
  PrestoConnectionError,
)

logger = logging.getLogger(__name__)


def _cmd_xml(name: str, **attrs: Optional[str]) -> str:
  """Build a <Cmd> XML string with optional attributes. Tag names/values per spec. None values are omitted."""
  parts = [f'<Cmd name="{name}"']
  for k, v in attrs.items():
    if v is not None:
      parts.append(f' {k}="{v}"')
  parts.append("/>\n")
  return "".join(parts)


def _text(el: Optional[ET.Element]) -> Optional[str]:
  return el.text.strip() if el is not None and el.text else None


class KingFisherPrestoBackend(MachineBackend):
  """Backend for the KingFisher Presto magnetic particle processor.

  Uses USB HID (VID/PID per Interface Specification). Protocol: XML Cmd/Res/Evt
  over 64-byte HID reports; event-based execution.
  """

  def __init__(
    self,
    vid: int = KINGFISHER_VID,
    pid: int = KINGFISHER_PID,
    serial_number: Optional[str] = None,
    on_event: Optional[Callable[[ET.Element], None]] = None,
  ):
    super().__init__()
    self._vid = vid
    self._pid = pid
    self._serial_number = serial_number
    self._conn = PrestoConnection(
      vid=vid,
      pid=pid,
      serial_number=serial_number,
      on_event=on_event,
    )
    self._instrument: Optional[str] = None
    self._version: Optional[str] = None
    self._serial: Optional[str] = None

  async def setup(self) -> None:
    """Open HID, send Connect, parse instrument/version/serial, start read loop."""
    await self._conn.setup()
    res = await self._conn.send_command(_cmd_xml("Connect"))
    self._instrument = _text(res.find("Instrument"))
    self._version = _text(res.find("Version"))
    self._serial = _text(res.find("Serial"))
    logger.info(
      "KingFisher Presto connected: %s, version %s, serial %s",
      self._instrument,
      self._version,
      self._serial,
    )

  async def stop(self) -> None:
    """Send Disconnect (instrument may not reply), stop read loop, close HID."""
    try:
      await self._conn.send_command(_cmd_xml("Disconnect"))
    except (PrestoConnectionError, asyncio.TimeoutError, asyncio.CancelledError):
      pass
    await self._conn.stop()
    self._instrument = None
    self._version = None
    self._serial = None

  async def connect(self, set_time: Optional[str] = None) -> None:
    """Send Connect command (e.g. to set time). Connection is already established by setup(). setTime: YYYY-MM-DD hh:mm:ss."""
    cmd = _cmd_xml("Connect", setTime=set_time) if set_time else _cmd_xml("Connect")
    res = await self._conn.send_command(cmd)
    self._instrument = _text(res.find("Instrument"))
    self._version = _text(res.find("Version"))
    self._serial = _text(res.find("Serial"))

  async def disconnect(self) -> None:
    """Send Disconnect. Instrument may not reply if connection already closed."""
    await self._conn.send_command(_cmd_xml("Disconnect"))

  async def get_status(
    self,
  ) -> dict:
    """Get instrument status. Returns dict with ok, status, error_code, error_text, error_code_description.

    status is \"Idle\", \"Busy\", or \"In error\" per spec. error_code_description is the standard
    description for the error code (Interface Specification 4.8) when known.
    """
    res = await self._conn.send_command(_cmd_xml("GetStatus"), raise_on_error=False)
    status_el = res.find("Status")
    status = _text(status_el) if status_el is not None else None
    ok = (res.get("ok") or "false").lower() == "true"
    err_el = res.find("Error")
    error_code = int(err_el.get("code", 0)) if err_el is not None and err_el.get("code") else None
    error_text = _text(err_el) if err_el is not None else None
    error_code_description = get_error_code_description(error_code) if error_code is not None else None
    return {
      "ok": ok,
      "status": status or "In error",
      "error_code": error_code,
      "error_text": error_text,
      "error_code_description": error_code_description,
    }

  async def get_protocol_time_left(self, protocol: Optional[str] = None) -> Dict[str, Optional[str]]:
    """Get time left of protocol or single-step execution. Returns time_left and time_to_pause (XML Duration).
    For single-step execution the spec omits TimeToPause; time_to_pause will be None."""
    cmd = _cmd_xml("GetProtocolTimeLeft", protocol=protocol)
    res = await self._conn.send_command(cmd, raise_on_error=False)
    time_left_el = res.find("TimeLeft")
    time_to_pause_el = res.find("TimeToPause")
    time_left = time_left_el.get("value") if time_left_el is not None else None
    time_to_pause = time_to_pause_el.get("value") if time_to_pause_el is not None else None
    return {"time_left": time_left, "time_to_pause": time_to_pause}

  async def list_protocols(self) -> Tuple[List[str], int]:
    """List protocols in instrument memory. Returns (protocol_names, memory_used_percent)."""
    res = await self._conn.send_command(_cmd_xml("ListProtocols"))
    protocols_el = res.find("Protocols")
    names: List[str] = []
    if protocols_el is not None:
      for p in protocols_el.findall("Protocol"):
        if p.text:
          names.append(p.text.strip())
    mem_el = res.find("MemoryUsed")
    memory_used = int(mem_el.get("value", 0)) if mem_el is not None else 0
    return (names, memory_used)

  async def download_protocol(self, name: str) -> bytes:
    """Download a protocol from instrument memory. Returns raw protocol bytes (base64-decoded)."""
    res = await self._conn.send_command(_cmd_xml("DownloadProtocol", protocol=name))
    cdata = "".join(res.itertext()).replace(" ", "").replace("\n", "").replace("\r", "")
    return base64.b64decode(cdata)

  async def upload_protocol(self, name: str, protocol_bytes: bytes, crc: Optional[int] = None) -> None:
    """Upload a protocol to instrument memory. crc: uint32 of BindIt protocol file data."""
    if crc is None:
      crc = binascii.crc32(protocol_bytes) & 0xFFFFFFFF
    b64 = base64.b64encode(protocol_bytes).decode("ascii")
    lines = [b64[i : i + 64] for i in range(0, len(b64), 64)]
    cdata_body = "\n        ".join(lines)
    cmd = (
      f'<Cmd name="UploadProtocol" protocol="{name}" crc="{crc}">\n'
      f"    <![CDATA[\n        {cdata_body}\n    ]]>\n</Cmd>\n"
    )
    await self._conn.send_command(cmd)

  async def get_event(self) -> ET.Element:
    """Return the next event from the queue. Blocks until one is available."""
    return await self._conn.get_event()

  def events(self):
    """Async generator of events. Use: async for evt in backend.events()."""
    return self._conn.events()

  async def acknowledge(self) -> None:
    """Send Acknowledge (e.g. after LoadPlate, RemovePlate, ChangePlate, Pause)."""
    await self._conn.send_command(_cmd_xml("Acknowledge"))

  async def error_acknowledge(self) -> None:
    """Send ErrorAcknowledge to clear instrument error state."""
    await self._conn.send_command(_cmd_xml("ErrorAcknowledge"))

  async def abort(self) -> None:
    """Two-phase abort: Feature report then Abort character. Stops execution and flushes buffers."""
    await self._conn.abort()

  async def start_protocol(
    self,
    protocol: str,
    tip: Optional[str] = None,
    step: Optional[str] = None,
  ) -> None:
    """Start a protocol or single step. Protocol must already be in instrument memory."""
    attrs: dict = {"protocol": protocol}
    if tip is not None:
      attrs["tip"] = tip
    if step is not None:
      attrs["step"] = step
    await self._conn.send_command(_cmd_xml("StartProtocol", **attrs))

  async def stop_protocol(self) -> None:
    """Stop ongoing protocol/step execution."""
    await self._conn.send_command(_cmd_xml("Stop"))

  @property
  def instrument(self) -> Optional[str]:
    return self._instrument

  @property
  def version(self) -> Optional[str]:
    return self._version

  @property
  def serial(self) -> Optional[str]:
    return self._serial

  def serialize(self) -> dict:
    return {
      **super().serialize(),
      "vid": self._vid,
      "pid": self._pid,
      "serial_number": self._serial_number,
    }
