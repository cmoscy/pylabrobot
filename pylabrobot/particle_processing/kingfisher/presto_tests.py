"""Unit tests for KingFisher Presto connection, backend, BDZ builder, and high-level API.

Tests _find_complete_message (XML message boundaries, nested Res/Evt) and
XML command building (_cmd_xml) per the KingFisher Presto Interface Specification.
Also tests protocol CRC-32 formula used for UploadProtocol (BDZ_FORMAT.md).
Tests BDZ builder output structure and high-level step methods (mix, dry, etc.).
"""

import asyncio
import gzip
import binascii
import warnings
import xml.etree.ElementTree as ET
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from .bdz_builder import (
  build_collect_beads_bdz,
  build_dry_bdz,
  build_mix_bdz,
  build_pause_bdz,
  build_release_beads_bdz,
)
from .presto_connection import _find_complete_message
from .presto_backend import _cmd_xml, KingFisherPrestoBackend
from .presto import KingFisherPresto


class TestFindCompleteMessage:
  """Tests for _find_complete_message: reassembly of Res/Evt from HID report stream."""

  def test_empty_buffer_returns_none(self):
    assert _find_complete_message(bytearray()) is None

  def test_partial_opening_tag_returns_none(self):
    assert _find_complete_message(bytearray(b"<Re")) is None
    assert _find_complete_message(bytearray(b"<Res")) is None
    assert _find_complete_message(bytearray(b"<Evt")) is None

  def test_self_closing_res_returns_message_and_end_index(self):
    msg = b'<Res name="GetStatus" ok="true"/>'
    buf = bytearray(msg)
    result = _find_complete_message(buf)
    assert result is not None
    found, end_idx = result
    assert found == msg
    assert end_idx == len(msg)

  def test_self_closing_evt_returns_message_and_end_index(self):
    msg = b'<Evt name="LoadPlate"/>'
    buf = bytearray(msg)
    result = _find_complete_message(buf)
    assert result is not None
    found, end_idx = result
    assert found == msg
    assert end_idx == len(msg)

  def test_res_with_body_returns_full_message(self):
    msg = b'<Res name="GetStatus" ok="true"><Status>Idle</Status></Res>'
    buf = bytearray(msg)
    result = _find_complete_message(buf)
    assert result is not None
    found, end_idx = result
    assert found == msg
    assert end_idx == len(msg)

  def test_nested_evt_returns_outer_message(self):
    msg = b'<Evt name="ChangePlate"><Evt name="RemovePlate"/></Evt>'
    buf = bytearray(msg)
    result = _find_complete_message(buf)
    assert result is not None
    found, end_idx = result
    assert found == msg
    assert end_idx == len(msg)

  def test_incomplete_nested_returns_none(self):
    buf = bytearray(b'<Evt name="Outer"><Evt name="Inner"/>')
    assert _find_complete_message(buf) is None

  def test_two_messages_returns_first_and_end_index(self):
    first = b'<Res name="Connect" ok="true"/>'
    second = b'<Evt name="LoadPlate"/>'
    buf = bytearray(first + second)
    result = _find_complete_message(buf)
    assert result is not None
    found, end_idx = result
    assert found == first
    assert end_idx == len(first)
    # After consuming first, second is found
    del buf[:end_idx]
    result2 = _find_complete_message(buf)
    assert result2 is not None
    found2, end_idx2 = result2
    assert found2 == second
    assert end_idx2 == len(second)

  def test_garbage_before_message_skipped(self):
    prefix = b"junk \x00\x00 "
    msg = b'<Res name="GetStatus" ok="true"/>'
    buf = bytearray(prefix + msg)
    result = _find_complete_message(buf)
    assert result is not None
    found, end_idx = result
    assert found == msg
    assert end_idx == len(prefix) + len(msg)

  def test_partial_second_message_returns_first_only(self):
    first = b'<Res name="Connect" ok="true"/>'
    partial = b"<Evt name=\"Load"
    buf = bytearray(first + partial)
    result = _find_complete_message(buf)
    assert result is not None
    found, end_idx = result
    assert found == first
    assert end_idx == len(first)


class TestCmdXml:
  """Tests for _cmd_xml: building <Cmd> XML strings per spec."""

  def test_connect_no_attrs(self):
    out = _cmd_xml("Connect")
    assert out == '<Cmd name="Connect"/>\n'

  def test_get_status(self):
    out = _cmd_xml("GetStatus")
    assert out == '<Cmd name="GetStatus"/>\n'

  def test_connect_with_set_time(self):
    out = _cmd_xml("Connect", setTime="2025-01-01 12:00:00")
    assert 'name="Connect"' in out
    assert 'setTime="2025-01-01 12:00:00"' in out
    assert out.endswith("/>\n")

  def test_start_protocol_with_protocol_attr(self):
    out = _cmd_xml("StartProtocol", protocol="MyProtocol")
    assert 'name="StartProtocol"' in out
    assert 'protocol="MyProtocol"' in out
    assert out.endswith("/>\n")

  def test_start_protocol_with_tip_and_step(self):
    out = _cmd_xml("StartProtocol", protocol="P", tip="T1", step="Step1")
    assert 'protocol="P"' in out
    assert 'tip="T1"' in out
    assert 'step="Step1"' in out

  def test_download_protocol(self):
    out = _cmd_xml("DownloadProtocol", protocol="SomeProtocol")
    assert 'name="DownloadProtocol"' in out
    assert 'protocol="SomeProtocol"' in out

  def test_none_attr_omitted(self):
    out = _cmd_xml("StartProtocol", protocol="P", step=None)
    assert 'protocol="P"' in out
    assert "step=" not in out

  def test_disconnect(self):
    out = _cmd_xml("Disconnect")
    assert out == '<Cmd name="Disconnect"/>\n'

  def test_acknowledge(self):
    out = _cmd_xml("Acknowledge")
    assert out == '<Cmd name="Acknowledge"/>\n'

  def test_list_protocols(self):
    out = _cmd_xml("ListProtocols")
    assert out == '<Cmd name="ListProtocols"/>\n'


class TestPrestoBackendGetProtocolTimeLeft:
  """Backend get_protocol_time_left: Cmd name and parsed dict from Res with TimeLeft/TimeToPause."""

  async def _run(self, backend, protocol=None):
    return await backend.get_protocol_time_left(protocol=protocol)

  def test_returns_time_left_only_single_step(self):
    backend = KingFisherPrestoBackend()
    res_xml = '<Res name="GetProtocolTimeLeft" ok="true"><TimeLeft value="PT2M42S"/></Res>'
    with patch.object(backend._conn, "send_command", new_callable=AsyncMock) as mock_send:
      mock_send.return_value = ET.fromstring(res_xml)
      result = asyncio.run(self._run(backend))
    assert result == {"time_left": "PT2M42S", "time_to_pause": None}
    call_args = mock_send.call_args
    assert "GetProtocolTimeLeft" in call_args[0][0]

  def test_returns_time_left_and_time_to_pause(self):
    backend = KingFisherPrestoBackend()
    res_xml = (
      '<Res name="GetProtocolTimeLeft" ok="true">'
      '<TimeLeft value="PT1M0S"/><TimeToPause value="PT30S"/>'
      "</Res>"
    )
    with patch.object(backend._conn, "send_command", new_callable=AsyncMock) as mock_send:
      mock_send.return_value = ET.fromstring(res_xml)
      result = asyncio.run(backend.get_protocol_time_left(protocol="MyProtocol"))
    assert result == {"time_left": "PT1M0S", "time_to_pause": "PT30S"}
    call_args = mock_send.call_args
    assert "GetProtocolTimeLeft" in call_args[0][0]
    assert "MyProtocol" in call_args[0][0]


class TestPrestoReconnectContinue:
  """Frontend: get_protocol_time_left, get_run_state, setup warning, continue_run, run_until_ready (no stop)."""

  def test_get_protocol_time_left_delegates_to_backend(self):
    mock_backend = MagicMock()
    mock_backend.get_protocol_time_left = AsyncMock(
      return_value={"time_left": "PT2M42S", "time_to_pause": None}
    )
    mock_backend._setup_finished = True
    presto = KingFisherPresto(backend=mock_backend)
    presto._setup_finished = True
    result = asyncio.run(presto.get_protocol_time_left())
    assert result == {"time_left": "PT2M42S", "time_to_pause": None}
    mock_backend.get_protocol_time_left.assert_called_once_with(None)

  def test_get_run_state_idle_message_and_no_warning(self):
    mock_backend = MagicMock()
    mock_backend.get_status = AsyncMock(
      return_value={"ok": True, "status": "Idle", "error_code": None, "error_text": None, "error_code_description": None}
    )
    mock_backend._setup_finished = True
    presto = KingFisherPresto(backend=mock_backend)
    presto._setup_finished = True
    with patch("pylabrobot.particle_processing.kingfisher.presto.warnings.warn") as mock_warn:
      result = asyncio.run(presto.get_run_state())
    assert result["status"] == "Idle"
    assert result["message"] == "No protocol in progress (ready for next command)."
    assert result["time_left"] is None
    assert result["time_to_pause"] is None
    mock_warn.assert_not_called()

  def test_get_run_state_busy_includes_time_left_and_message(self):
    mock_backend = MagicMock()
    mock_backend.get_status = AsyncMock(
      return_value={"ok": True, "status": "Busy", "error_code": None, "error_text": None, "error_code_description": None}
    )
    mock_backend.get_protocol_time_left = AsyncMock(
      return_value={"time_left": "PT2M42S", "time_to_pause": None}
    )
    mock_backend._setup_finished = True
    presto = KingFisherPresto(backend=mock_backend)
    presto._setup_finished = True
    with patch("pylabrobot.particle_processing.kingfisher.presto.warnings.warn") as mock_warn:
      result = asyncio.run(presto.get_run_state())
    assert result["status"] == "Busy"
    assert "continue_run" in result["message"]
    assert result["time_left"] == "PT2M42S"
    mock_warn.assert_not_called()

  def test_setup_when_idle_no_warning(self):
    mock_backend = MagicMock()
    mock_backend.setup = AsyncMock()
    mock_backend._setup_finished = True
    presto = KingFisherPresto(backend=mock_backend)
    with patch.object(presto, "get_run_state", new_callable=AsyncMock) as mock_gr:
      mock_gr.return_value = {
        "status": "Idle",
        "message": "No protocol in progress (ready for next command).",
        "time_left": None,
        "time_to_pause": None,
      }
      with patch("pylabrobot.particle_processing.kingfisher.presto.warnings.warn") as mock_warn:
        asyncio.run(presto.setup())
    mock_warn.assert_not_called()

  def test_setup_when_busy_warns_with_continue_run_and_time_left(self):
    mock_backend = MagicMock()
    mock_backend.setup = AsyncMock()
    mock_backend._setup_finished = True
    presto = KingFisherPresto(backend=mock_backend)
    with patch.object(presto, "get_run_state", new_callable=AsyncMock) as mock_gr:
      mock_gr.return_value = {
        "status": "Busy",
        "message": "Protocol in progress. Call continue_run() to attach to the event stream. Time left: PT2M42S.",
        "time_left": "PT2M42S",
        "time_to_pause": None,
      }
      with patch("pylabrobot.particle_processing.kingfisher.presto.warnings.warn") as mock_warn:
        asyncio.run(presto.setup())
    mock_warn.assert_called_once()
    call_args = mock_warn.call_args[0]
    assert "continue_run" in call_args[0]
    assert "PT2M42S" in call_args[0]

  async def _consume_run_until_ready(self, presto):
    items = []
    async for name, evt, ack in presto.run_until_ready():
      items.append((name, evt, ack))
    return items

  async def _consume_continue_run(self, presto):
    items = []
    async for name, evt, ack in presto.continue_run():
      items.append((name, evt, ack))
    return items

  def test_continue_run_when_idle_yields_ready_once(self):
    mock_backend = MagicMock()
    mock_backend.get_status = AsyncMock(
      return_value={"ok": True, "status": "Idle", "error_code": None, "error_text": None, "error_code_description": None}
    )
    mock_backend._setup_finished = True
    presto = KingFisherPresto(backend=mock_backend)
    presto._setup_finished = True
    items = asyncio.run(self._consume_continue_run(presto))
    assert items == [("Ready", None, None)]

  def test_continue_run_when_busy_no_stop_protocol(self):
    mock_backend = MagicMock()
    mock_backend.get_status = AsyncMock(
      return_value={"ok": True, "status": "Busy", "error_code": None, "error_text": None, "error_code_description": None}
    )
    evt_ready = ET.fromstring('<Evt name="Ready"/>')
    async def event_gen():
      yield evt_ready
    mock_backend.events = lambda: event_gen()
    mock_backend._setup_finished = True
    mock_backend.stop_protocol = AsyncMock()
    presto = KingFisherPresto(backend=mock_backend)
    presto._setup_finished = True
    items = asyncio.run(self._consume_continue_run(presto))
    assert len(items) == 1
    assert items[0][0] == "Ready"
    mock_backend.stop_protocol.assert_not_called()

  def test_run_until_ready_does_not_call_stop_protocol(self):
    mock_backend = MagicMock()
    evt_ready = ET.fromstring('<Evt name="Ready"/>')
    async def event_gen():
      yield evt_ready
    mock_backend.events = lambda: event_gen()
    mock_backend._setup_finished = True
    mock_backend.stop_protocol = AsyncMock()
    presto = KingFisherPresto(backend=mock_backend)
    presto._setup_finished = True
    items = asyncio.run(self._consume_run_until_ready(presto))
    assert len(items) == 1
    assert items[0][0] == "Ready"
    mock_backend.stop_protocol.assert_not_called()


class TestProtocolCrc32:
  """Protocol CRC-32 formula (UploadProtocol / BDZ_FORMAT.md): deterministic, no seed."""

  def test_crc32_deterministic(self):
    data = b"fake.bdz payload"
    crc1 = binascii.crc32(data) & 0xFFFFFFFF
    crc2 = binascii.crc32(data) & 0xFFFFFFFF
    assert crc1 == crc2

  def test_crc32_unsigned_32bit(self):
    data = b"x"
    crc = binascii.crc32(data) & 0xFFFFFFFF
    assert 0 <= crc <= 0xFFFFFFFF and isinstance(crc, int)

  def test_crc32_known_value(self):
    # Fixed byte sequence (bdz magic + "BindIt Software") -> reproducible CRC
    magic = bytes.fromhex("b6751cf2") + b"BindIt Software"
    crc = binascii.crc32(magic) & 0xFFFFFFFF
    assert crc == 3884336888


def _decompress_bdz_blocks(bdz: bytes) -> tuple[bytes, bytes]:
  """Return (properties_xml, exported_data_xml) from .bdz bytes. Header = 61 bytes."""
  assert bdz[:4] == bytes.fromhex("b6751cf2"), "bad magic"
  rest = bdz[61:]
  assert rest[:2] == b"\x1f\x8b", "first block not gzip"
  # Second gzip block starts at next 1f 8b (skip first occurrence)
  idx = rest.find(b"\x1f\x8b", 2)
  assert idx > 0, "no second gzip block"
  dec1 = gzip.decompress(rest[:idx])
  dec2 = gzip.decompress(rest[idx:])
  return (dec1, dec2)


class TestBdzBuilder:
  """BDZ builder: output has correct magic, two gzip blocks, ExportedData contains step."""

  def test_build_mix_bdz_structure(self):
    bdz = build_mix_bdz("plr_Mix", "Wash1", 30.0, "Fast")
    assert bdz[:4] == bytes.fromhex("b6751cf2")
    assert bdz[18:33] == b"BindIt Software"
    props, exported = _decompress_bdz_blocks(bdz)
    props_str = props.decode("utf-8")
    assert "<Properties" in props_str
    assert "plr_Mix" in props_str
    assert b"<ExportedData" in exported
    assert b"<Mix " in exported
    assert b"Wash1" in exported
    assert b"PT30S" in exported

  def test_build_dry_bdz_structure(self):
    bdz = build_dry_bdz("plr_Dry", 300.0, plate_name="Plate1")
    assert bdz[:4] == bytes.fromhex("b6751cf2")
    props, exported = _decompress_bdz_blocks(bdz)
    assert b"<Dry " in exported
    assert b"Plate1" in exported
    assert b"PT5M" in exported or b"Duration" in exported.decode("utf-8")

  def test_build_collect_beads_bdz_structure(self):
    bdz = build_collect_beads_bdz("plr_CollectBeads", 3, 30.0, plate_name="P1")
    assert bdz[:4] == bytes.fromhex("b6751cf2")
    props, exported = _decompress_bdz_blocks(bdz)
    assert b"CollectBeads" in exported
    assert b"P1" in exported
    assert b"<Count>3</Count>" in exported

  def test_build_release_beads_bdz_structure(self):
    bdz = build_release_beads_bdz("plr_ReleaseBeads", 10.0, "Fast", plate_name="P1")
    assert bdz[:4] == bytes.fromhex("b6751cf2")
    props, exported = _decompress_bdz_blocks(bdz)
    assert b"ReleaseBeads" in exported
    assert b"P1" in exported

  def test_build_pause_bdz_structure(self):
    bdz = build_pause_bdz("plr_Pause", "Wait for user")
    assert bdz[:4] == bytes.fromhex("b6751cf2")
    props, exported = _decompress_bdz_blocks(bdz)
    assert b"<Pause " in exported
    assert b"Wait for user" in exported

  def test_mix_unsupported_speed_raises(self):
    with pytest.raises(ValueError, match="Slow.*not supported"):
      build_mix_bdz("plr_Mix", "Wash1", 30.0, "Slow")
    with pytest.raises(ValueError, match="Bottom mix.*not supported"):
      build_mix_bdz("plr_Mix", "Wash1", 30.0, "Bottom mix")

  def test_collect_beads_count_out_of_range_raises(self):
    with pytest.raises(ValueError, match="1..5"):
      build_collect_beads_bdz("plr_CollectBeads", 0, 30.0)
    with pytest.raises(ValueError, match="1..5"):
      build_collect_beads_bdz("plr_CollectBeads", 6, 30.0)

  def test_deterministic_same_inputs_same_bdz(self):
    b1 = build_mix_bdz("plr_Mix", "Wash1", 30.0, "Medium")
    b2 = build_mix_bdz("plr_Mix", "Wash1", 30.0, "Medium")
    assert b1 == b2


class TestPrestoHighLevelApi:
  """High-level step methods (mix, dry, etc.) call builder, upload, start with correct slot."""

  def test_mix_upload_and_start_with_plr_mix_when_wait_until_ready_false(self):
    mock_backend = MagicMock()
    mock_backend.upload_protocol = AsyncMock(return_value=None)
    mock_backend.start_protocol = AsyncMock(return_value=None)
    mock_backend._setup_finished = True
    presto = KingFisherPresto(backend=mock_backend)
    presto._setup_finished = True
    asyncio.run(presto.mix("Wash1", 30.0, speed="Fast", wait_until_ready=False))
    mock_backend.upload_protocol.assert_called_once()
    call_args = mock_backend.upload_protocol.call_args
    assert call_args[0][0] == "plr_Mix"
    assert isinstance(call_args[0][1], bytes)
    assert call_args[0][1][:4] == bytes.fromhex("b6751cf2")
    mock_backend.start_protocol.assert_called_once_with("plr_Mix", tip=None, step=None)

  def test_dry_upload_and_start_with_plr_dry(self):
    mock_backend = MagicMock()
    mock_backend.upload_protocol = AsyncMock(return_value=None)
    mock_backend.start_protocol = AsyncMock(return_value=None)
    mock_backend._setup_finished = True
    presto = KingFisherPresto(backend=mock_backend)
    presto._setup_finished = True
    asyncio.run(presto.dry(60.0, plate="Plate1", wait_until_ready=False))
    mock_backend.upload_protocol.assert_called_once()
    assert mock_backend.upload_protocol.call_args[0][0] == "plr_Dry"
    mock_backend.start_protocol.assert_called_once_with("plr_Dry", tip=None, step=None)

  def test_collect_beads_upload_and_start_with_plr_collect_beads(self):
    mock_backend = MagicMock()
    mock_backend.upload_protocol = AsyncMock(return_value=None)
    mock_backend.start_protocol = AsyncMock(return_value=None)
    mock_backend._setup_finished = True
    presto = KingFisherPresto(backend=mock_backend)
    presto._setup_finished = True
    asyncio.run(presto.collect_beads(count=3, collect_time_sec=30.0, wait_until_ready=False))
    assert mock_backend.upload_protocol.call_args[0][0] == "plr_CollectBeads"
    mock_backend.start_protocol.assert_called_once_with("plr_CollectBeads", tip=None, step=None)
