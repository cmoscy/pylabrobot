"""High-level KingFisher Presto machine frontend wrapping the backend.

Same lifecycle as other machines (setup(), stop(), async with).
Exposes start_protocol(), get_status(), acknowledge(), error_acknowledge(),
and async for evt in backend.events() for event-based orchestration.
"""

import xml.etree.ElementTree as ET
from typing import List, Optional, Tuple

from pylabrobot.machines.machine import Machine, need_setup_finished

from .presto_backend import KingFisherPrestoBackend


class KingFisherPresto(Machine):
  """High-level KingFisher Presto magnetic particle processor.

  Wraps KingFisherPrestoBackend; same API pattern as Thermocycler/Machine.
  Use async for evt in self.backend.events() to react to LoadPlate, RemovePlate, etc.
  """

  def __init__(self, backend: KingFisherPrestoBackend):
    super().__init__(backend=backend)
    self.backend: KingFisherPrestoBackend = backend

  @need_setup_finished
  async def get_status(self) -> dict:
    """Get instrument status: Idle, Busy, or In error. Returns dict with ok, status, error_code, error_text, error_code_description."""
    return await self.backend.get_status()

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
