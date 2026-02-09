"""
KingFisher Presto connection layer: HID transport, XML framing, message loop, event queue.

Uses KingFisherHID (subclass of pylabrobot.io.hid.HID) which adds send_feature_report
for Abort/flow control only in this package; the generic io.hid API is unchanged.

Protocol: 64-byte Output report (byte 0 = payload length, bytes 1–63 = payload);
command termination newline (ASCII 10); messages <Cmd>, <Res>, <Evt>; demux Res to
pending command, Evt to queue/callback. See KingFisher Presto Interface Specification.
"""

import asyncio
import logging
import xml.etree.ElementTree as ET
from typing import Callable, Optional, Tuple

from .error_codes import format_error_message
from .presto_hid import KingFisherHID

logger = logging.getLogger(__name__)

REPORT_SIZE = 64
PAYLOAD_MAX = 63
CMD_TERMINATOR = b"\n"
KINGFISHER_VID = 0x0AB6
KINGFISHER_PID = 0x02C9
HID_READ_TIMEOUT_MS = 5000
# Abort: two-byte Feature report via control endpoint (spec 3.2.3): first byte nonzero, second zero.
ABORT_FEATURE_REPORT = bytes([0x01, 0x00])
ABORT_PAYLOAD = bytes([0x1B, 0x0A])


def _find_complete_message(buffer: bytearray) -> Optional[Tuple[bytes, int]]:
  """Find the first complete XML root element (Res or Evt) in buffer.

  Returns (message_bytes, end_index) or None if no complete message.
  Handles nested elements (e.g. <Evt name="ChangePlate"><Evt name="RemovePlate"/>...</Evt>).
  """
  i = 0
  while i < len(buffer):
    r = buffer.find(b"<Res", i)
    e = buffer.find(b"<Evt", i)
    if r == -1 and e == -1:
      return None
    start = min(x for x in (r, e) if x != -1) if (r != -1 and e != -1) else (r if r != -1 else e)
    end_angle = buffer.find(b">", start)
    if end_angle == -1:
      return None
    between = buffer[start:end_angle]
    if between.rstrip().endswith(b"/"):
      return (bytes(buffer[start : end_angle + 1]), end_angle + 1)
    if b"<Res" in between:
      close_tag = b"</Res>"
      open_tag = b"<Res"
    else:
      close_tag = b"</Evt>"
      open_tag = b"<Evt"
    depth = 1
    pos = end_angle + 1
    while depth > 0 and pos < len(buffer):
      next_close = buffer.find(close_tag, pos)
      if next_close == -1:
        return None
      next_open = buffer.find(open_tag, pos)
      if next_open != -1 and next_open < next_close:
        end_a = buffer.find(b">", next_open)
        if end_a == -1:
          return None
        bet = buffer[next_open:end_a]
        if not bet.rstrip().endswith(b"/"):
          depth += 1
        pos = end_a + 1
      else:
        depth -= 1
        pos = next_close + len(close_tag)
    if depth == 0:
      return (bytes(buffer[start:pos]), pos)
    i = pos
  return None


class PrestoConnectionError(Exception):
  """Raised when the instrument returns Res@ok=\"false\" or communication fails."""

  def __init__(self, message: str, code: Optional[int] = None, res_name: Optional[str] = None):
    super().__init__(message)
    self.code = code
    self.res_name = res_name


class PrestoConnection:
  """Connection to a KingFisher Presto over USB HID: framing, message loop, event queue."""

  def __init__(
    self,
    vid: int = KINGFISHER_VID,
    pid: int = KINGFISHER_PID,
    serial_number: Optional[str] = None,
    on_event: Optional[Callable[[ET.Element], None]] = None,
  ):
    self._hid = KingFisherHID(vid=vid, pid=pid, serial_number=serial_number)
    self._on_event = on_event
    self._event_queue: asyncio.Queue[ET.Element] = asyncio.Queue()
    self._pending_future: Optional[asyncio.Future[ET.Element]] = None
    self._read_task: Optional[asyncio.Task[None]] = None
    self._buffer = bytearray()
    self._send_lock = asyncio.Lock()
    self._stopping = False

  async def setup(self) -> None:
    """Open HID and start the background read loop.

    Idempotent: if already set up (read loop running), returns immediately so callers
    can re-run setup to refresh instrument state (e.g. send Connect again) without
    re-opening the device (which would raise HIDException).
    """
    if self._read_task is not None and not self._read_task.done():
      logger.debug("KingFisher Presto connection: already set up, skipping open.")
      return
    await self._hid.setup()
    self._stopping = False
    self._read_task = asyncio.create_task(self._read_loop())
    logger.debug("KingFisher Presto connection: HID opened, read loop started.")

  async def stop(self) -> None:
    """Stop the read loop and close HID. Tolerates no Disconnect response (spec)."""
    self._stopping = True
    if self._read_task is not None:
      self._read_task.cancel()
      try:
        await self._read_task
      except asyncio.CancelledError:
        pass
      self._read_task = None
    if self._pending_future is not None and not self._pending_future.done():
      self._pending_future.set_exception(asyncio.CancelledError())
      self._pending_future = None
    await self._hid.stop()
    logger.debug("KingFisher Presto connection: HID closed.")

  async def _send_payload(self, payload: bytes) -> None:
    """Send payload as 64-byte HID reports (async)."""
    if not payload.endswith(CMD_TERMINATOR):
      payload = payload + CMD_TERMINATOR
    offset = 0
    while offset < len(payload):
      chunk = payload[offset : offset + PAYLOAD_MAX]
      length = len(chunk)
      report = bytes([length]) + chunk.ljust(PAYLOAD_MAX, b"\x00")[:PAYLOAD_MAX]
      assert len(report) == REPORT_SIZE
      await self._hid.write(report, report_id=b"\x00")
      offset += length

  async def _read_loop(self) -> None:
    """Background task: read HID reports, reassemble XML, demux Res vs Evt."""
    while not self._stopping:
      try:
        raw = await self._hid.read(size=REPORT_SIZE, timeout=HID_READ_TIMEOUT_MS)
      except asyncio.CancelledError:
        break
      except Exception as e:
        if self._stopping:
          break
        logger.warning("KingFisher Presto read_loop read error: %s", e)
        continue
      if not raw:
        continue
      length = raw[0] if len(raw) > 0 else 0
      if length > 0:
        self._buffer.extend(raw[1 : 1 + min(length, len(raw) - 1)])
      while True:
        result = _find_complete_message(self._buffer)
        if result is None:
          break
        msg_bytes, end_idx = result
        del self._buffer[:end_idx]
        try:
          root = ET.fromstring(msg_bytes.decode("utf-8"))
        except ET.ParseError as e:
          logger.warning("KingFisher Presto parse error: %s", e)
          continue
        tag = (root.tag or "").split("}")[-1]
        if tag == "Res":
          if self._pending_future is not None and not self._pending_future.done():
            self._pending_future.set_result(root)
            self._pending_future = None
          else:
            logger.debug("Orphan Res: %s", ET.tostring(root, encoding="unicode")[:200])
        elif tag == "Evt":
          self._event_queue.put_nowait(root)
          if self._on_event is not None:
            try:
              self._on_event(root)
            except Exception as e:
              logger.warning("KingFisher Presto on_event callback error: %s", e)

  async def send_command(self, cmd_xml: str, raise_on_error: bool = True) -> ET.Element:
    """Send a <Cmd> XML string, wait for the matching <Res>, return parsed Res element.

    Only one command may be in flight. Events received while waiting are queued.
    If raise_on_error is True (default), raises PrestoConnectionError when Res@ok=\"false\".
    If False, returns the Res element so the caller can inspect Error/Warning.
    """
    async with self._send_lock:
      if self._pending_future is not None and not self._pending_future.done():
        await self._pending_future
      loop = asyncio.get_running_loop()
      self._pending_future = loop.create_future()
      await self._send_payload(cmd_xml.encode("utf-8"))
      try:
        res = await asyncio.wait_for(self._pending_future, timeout=30.0)
      except asyncio.TimeoutError:
        self._pending_future = None
        raise PrestoConnectionError("Timeout waiting for response") from None
      self._pending_future = None
      if raise_on_error:
        ok = res.get("ok", "false")
        if ok and ok.lower() == "false":
          err = res.find("Error")
          code = int(err.get("code", 0)) if err is not None and err.get("code") else None
          instrument_text = (err.text or "").strip() if err is not None else None
          message = format_error_message(code, instrument_text, kind="error")
          raise PrestoConnectionError(message, code=code, res_name=res.get("name"))
      return res

  async def send_without_response(self, cmd_xml: str) -> None:
    """Send a <Cmd> XML string without waiting for <Res>. Use for Rotate when completion is signaled by Evt (Ready/Error).

    Holds the send lock so no other command is in flight. Any Res that arrives for this
    command is treated as orphaned (logged). Caller should then wait for completion via get_event().
    """
    async with self._send_lock:
      if self._pending_future is not None and not self._pending_future.done():
        await self._pending_future
      await self._send_payload(cmd_xml.encode("utf-8"))

  async def get_event(self) -> ET.Element:
    """Return the next event from the queue. Blocks until one is available."""
    return await self._event_queue.get()

  async def abort(self) -> None:
    """Two-phase abort per spec 3.2.3, 5.1: Feature report then Abort character."""
    await self._hid.send_feature_report(ABORT_FEATURE_REPORT)
    await self._send_payload(ABORT_PAYLOAD)

  async def events(self):
    """Async generator yielding events from the queue. Use: async for evt in connection.events()."""
    while not self._stopping:
      try:
        evt = await asyncio.wait_for(self._event_queue.get(), timeout=1.0)
        yield evt
      except asyncio.TimeoutError:
        continue
      except asyncio.CancelledError:
        break
