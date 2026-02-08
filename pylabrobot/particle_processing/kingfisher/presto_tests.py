"""Unit tests for KingFisher Presto connection, backend, BDZ builder, and high-level API.

Tests _find_complete_message (XML message boundaries, nested Res/Evt) and
XML command building (_cmd_xml) per the KingFisher Presto Interface Specification.
Also tests protocol CRC-32 formula used for UploadProtocol (BDZ_FORMAT.md).
Tests BDZ builder output structure and run_protocol (protocol-based run).
"""

import asyncio
import binascii
from pathlib import Path
import xml.etree.ElementTree as ET
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from .bdz_builder import (
  build_collect_beads_bdz,
  build_dry_bdz,
  build_mix_bdz,
  build_pause_bdz,
  build_release_beads_bdz,
  read_bdz,
)
from .kingfisher_protocol import (
  KingFisherProtocol,
  Plate,
  PlateType,
  Tip,
  TipPosition,
  Image,
  parse_bdz_to_protocol,
)
from .presto_connection import _find_complete_message
from .presto_backend import _cmd_xml, KingFisherPrestoBackend, TurntableLocation
from .presto import KingFisherPresto
from .presto_connection import PrestoConnectionError


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

  def test_rotate(self):
    out = _cmd_xml("Rotate", nest="1", position="2")
    assert 'name="Rotate"' in out
    assert 'nest="1"' in out
    assert 'position="2"' in out

  def test_rotate_processing_maps_to_spec_position_1(self):
    """rotate(position=1, location='processing') -> nest=1, position=1."""
    out = _cmd_xml("Rotate", nest="1", position="1")
    assert 'nest="1"' in out
    assert 'position="1"' in out

  def test_rotate_loading_maps_to_spec_position_2(self):
    """rotate(position=1, location='loading') -> nest=1, position=2."""
    out = _cmd_xml("Rotate", nest="1", position="2")
    assert 'nest="1"' in out
    assert 'position="2"' in out


class TestTurntableState:
  """Turntable state: unknown after init/setup; updated on Ready; not updated on Error."""

  def test_get_turntable_state_after_init_returns_none_for_both(self):
    backend = KingFisherPrestoBackend()
    assert backend.get_turntable_state() == {1: None, 2: None}

  def test_get_turntable_state_after_setup_returns_none_for_both(self):
    backend = KingFisherPrestoBackend()
    with patch.object(backend._conn, "setup", new_callable=AsyncMock), patch.object(
      backend._conn, "send_command", new_callable=AsyncMock
    ) as mock_send:
      mock_send.return_value = ET.fromstring(
        '<Res name="Connect" ok="true"><Instrument>KF</Instrument><Version>1</Version><Serial>123</Serial></Res>'
      )
      asyncio.run(backend.setup())
    assert backend.get_turntable_state() == {1: None, 2: None}

  def test_rotate_position_1_location_loading_sends_nest_1_position_2(self):
    backend = KingFisherPrestoBackend()
    evt_ready = ET.fromstring('<Evt name="Ready"/>')
    with patch.object(backend._conn, "send_command", new_callable=AsyncMock) as mock_send, patch.object(
      backend._conn, "get_event", new_callable=AsyncMock, side_effect=[evt_ready]
    ):
      asyncio.run(backend.rotate(position=1, location="loading"))
    call_args = mock_send.call_args[0][0]
    assert "nest=\"1\"" in call_args
    assert "position=\"2\"" in call_args
    assert backend.get_turntable_state() == {1: "loading", 2: "processing"}

  def test_rotate_position_1_location_processing_sends_nest_1_position_1(self):
    backend = KingFisherPrestoBackend()
    evt_ready = ET.fromstring('<Evt name="Ready"/>')
    with patch.object(backend._conn, "send_command", new_callable=AsyncMock), patch.object(
      backend._conn, "get_event", new_callable=AsyncMock, side_effect=[evt_ready]
    ):
      asyncio.run(backend.rotate(position=1, location="processing"))
    assert backend.get_turntable_state() == {1: "processing", 2: "loading"}

  def test_rotate_on_error_does_not_update_state_and_raises(self):
    backend = KingFisherPrestoBackend()
    evt_error = ET.fromstring('<Evt name="Error"><Error code="5">Turntable position error.</Error></Evt>')
    with patch.object(backend._conn, "send_command", new_callable=AsyncMock), patch.object(
      backend._conn, "get_event", new_callable=AsyncMock, side_effect=[evt_error]
    ), patch.object(backend, "error_acknowledge", new_callable=AsyncMock) as mock_ack:
      with pytest.raises(PrestoConnectionError) as exc_info:
        asyncio.run(backend.rotate(position=1, location="loading"))
    assert backend.get_turntable_state() == {1: None, 2: None}
    mock_ack.assert_called_once()
    assert "Turntable position error" in str(exc_info.value) or "5" in str(exc_info.value)

  def test_stop_clears_turntable_state(self):
    backend = KingFisherPrestoBackend()
    backend._position_at_processing = 1
    with patch.object(backend._conn, "send_command", new_callable=AsyncMock), patch.object(
      backend._conn, "stop", new_callable=AsyncMock
    ):
      asyncio.run(backend.stop())
    assert backend.get_turntable_state() == {1: None, 2: None}

  def test_load_plate_when_unknown_raises_value_error(self):
    backend = KingFisherPrestoBackend()
    assert backend._position_at_processing is None
    with pytest.raises(ValueError, match="Turntable state unknown"):
      asyncio.run(backend.load_plate())

  def test_load_plate_when_known_calls_rotate_position_at_loading_to_processing(self):
    backend = KingFisherPrestoBackend()
    backend._position_at_processing = 1  # position 1 at processing, so position 2 at loading
    evt_ready = ET.fromstring('<Evt name="Ready"/>')
    with patch.object(backend._conn, "send_command", new_callable=AsyncMock) as mock_send, patch.object(
      backend._conn, "get_event", new_callable=AsyncMock, side_effect=[evt_ready]
    ):
      asyncio.run(backend.load_plate())
    # load_plate() should rotate position 2 to processing (nest=2, position=1)
    call_args = mock_send.call_args[0][0]
    assert "nest=\"2\"" in call_args
    assert "position=\"1\"" in call_args
    assert backend.get_turntable_state() == {1: "loading", 2: "processing"}

  def test_setup_initialize_turntable_true_calls_rotate_1_processing(self):
    backend = KingFisherPrestoBackend()
    evt_ready = ET.fromstring('<Evt name="Ready"/>')
    connect_res = ET.fromstring(
      '<Res name="Connect" ok="true"><Instrument>KF</Instrument><Version>1</Version><Serial>123</Serial></Res>'
    )
    rotate_res = ET.fromstring('<Res name="Rotate" ok="true"/>')
    with patch.object(backend._conn, "setup", new_callable=AsyncMock), patch.object(
      backend._conn, "send_command", new_callable=AsyncMock
    ) as mock_send, patch.object(backend._conn, "get_event", new_callable=AsyncMock, return_value=evt_ready):
      mock_send.side_effect = [connect_res, rotate_res]
      asyncio.run(backend.setup(initialize_turntable=True))
    assert backend._position_at_processing == 1
    assert backend.get_turntable_state() == {1: "processing", 2: "loading"}


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
  """Frontend: get_protocol_time_left, get_run_state, setup warning, next_event (no run_until_ready/continue_run)."""

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
    assert "existing run" in result["message"] and ("next_event" in result["message"] or "stop_protocol" in result["message"])
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

  def test_setup_when_busy_warns_with_next_event_and_time_left(self):
    mock_backend = MagicMock()
    mock_backend.setup = AsyncMock()
    mock_backend._setup_finished = True
    presto = KingFisherPresto(backend=mock_backend)
    with patch.object(presto, "get_run_state", new_callable=AsyncMock) as mock_gr:
      mock_gr.return_value = {
        "status": "Busy",
        "message": "Protocol in progress (existing run). Attach to continue with next_event(), or stop_protocol() or abort() to stop. Time left: PT2M42S.",
        "time_left": "PT2M42S",
        "time_to_pause": None,
      }
      with patch("pylabrobot.particle_processing.kingfisher.presto.warnings.warn") as mock_warn:
        asyncio.run(presto.setup())
    mock_warn.assert_called_once()
    call_args = mock_warn.call_args[0]
    assert "existing protocol" in call_args[0] or "attach" in call_args[0]
    assert "PT2M42S" in call_args[0]

  async def _drain_next_event_loop(self, presto, *, attach: bool = False):
    """Loop next_event() until Ready/Aborted/Error; return list of (name, evt, ack)."""
    items = []
    while True:
      name, evt, ack = await presto.next_event(attach=attach)
      items.append((name, evt, ack))
      if name in ("Ready", "Aborted", "Error"):
        break
      attach = False
    return items

  def test_next_event_attach_when_idle_drain_returns_ready_once(self):
    """Draining with next_event(attach=True) when Idle returns [(Ready, None, None)] without reading queue."""
    mock_backend = MagicMock()
    mock_backend.get_status = AsyncMock(
      return_value={"ok": True, "status": "Idle", "error_code": None, "error_text": None, "error_code_description": None}
    )
    mock_backend._setup_finished = True
    presto = KingFisherPresto(backend=mock_backend)
    presto._setup_finished = True
    items = asyncio.run(self._drain_next_event_loop(presto, attach=True))
    assert items == [("Ready", None, None)]

  def test_next_event_attach_when_busy_no_stop_protocol(self):
    """Draining with next_event(attach=True) when Busy consumes events until Ready; does not call stop_protocol."""
    mock_backend = MagicMock()
    mock_backend.get_status = AsyncMock(
      return_value={"ok": True, "status": "Busy", "error_code": None, "error_text": None, "error_code_description": None}
    )
    evt_ready = ET.fromstring('<Evt name="Ready"/>')
    mock_backend.get_event = AsyncMock(side_effect=[evt_ready])
    mock_backend._setup_finished = True
    mock_backend.stop_protocol = AsyncMock()
    presto = KingFisherPresto(backend=mock_backend)
    presto._setup_finished = True
    items = asyncio.run(self._drain_next_event_loop(presto, attach=True))
    assert len(items) == 1
    assert items[0][0] == "Ready"
    mock_backend.stop_protocol.assert_not_called()

  def test_next_event_loop_does_not_call_stop_protocol(self):
    """Draining with next_event() until Ready does not call stop_protocol."""
    mock_backend = MagicMock()
    evt_ready = ET.fromstring('<Evt name="Ready"/>')
    mock_backend.get_event = AsyncMock(side_effect=[evt_ready])
    mock_backend._setup_finished = True
    mock_backend.stop_protocol = AsyncMock()
    presto = KingFisherPresto(backend=mock_backend)
    presto._setup_finished = True
    items = asyncio.run(self._drain_next_event_loop(presto))
    assert len(items) == 1
    assert items[0][0] == "Ready"
    mock_backend.stop_protocol.assert_not_called()

  def test_next_event_returns_one_event(self):
    """next_event() returns one (name, evt, ack) from the event queue."""
    mock_backend = MagicMock()
    evt_load = ET.fromstring('<Evt name="LoadPlate" plate="Plate1"/>')
    mock_backend.get_event = AsyncMock(side_effect=[evt_load])
    mock_backend._setup_finished = True
    presto = KingFisherPresto(backend=mock_backend)
    presto._setup_finished = True
    name, evt, ack = asyncio.run(presto.next_event())
    assert name == "LoadPlate"
    assert evt is evt_load
    assert ack is not None and callable(ack)

  def test_next_event_attach_when_idle_returns_ready_without_reading_queue(self):
    """next_event(attach=True) when status is Idle returns (Ready, None, None) without reading queue."""
    mock_backend = MagicMock()
    mock_backend.get_status = AsyncMock(
      return_value={"ok": True, "status": "Idle", "error_code": None, "error_text": None, "error_code_description": None}
    )
    mock_backend.get_event = AsyncMock()
    mock_backend._setup_finished = True
    presto = KingFisherPresto(backend=mock_backend)
    presto._setup_finished = True
    name, evt, ack = asyncio.run(presto.next_event(attach=True))
    assert name == "Ready"
    assert evt is None
    assert ack is None
    mock_backend.get_event.assert_not_called()


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
  """Return (properties_xml, exported_data_xml) from .bdz bytes. Uses canonical reader."""
  from .bdz_builder import decompress_bdz
  return decompress_bdz(bdz)


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
    assert b"PT5M" in exported or b"Duration" in exported

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


class TestKingFisherProtocol:
  """KingFisher protocol representation: build protocol, to_bdz, roundtrip from example BDZ."""

  def test_build_protocol_like_96_well_to_bdz(self):
    """Build a protocol with one tip, 7 steps, 4 plates; to_bdz produces valid structure."""
    p = KingFisherProtocol(name="test96")
    tips_plate = Plate.create("Tips", PlateType.TIPS_96)
    dwp = Plate.create("96 DWP", PlateType.DWP_96)
    wp1 = Plate.create("96WP", PlateType.WP_96)
    wp2 = Plate.create("96 WP", PlateType.WP_96)
    p.add_tip(Tip(name="Tip1", plate=tips_plate, steps=[]))
    p.add_plate(dwp)
    p.add_plate(wp1)
    p.add_plate(wp2)
    p.add_collect_beads("CollectBeads1", "96 DWP", 3, 5.0)
    p.add_release_beads("ReleaseBeads1", "96 DWP", 5.0, "Fast")
    p.add_mix(
      "Mix1", "96 DWP",
      image=Image.Heating, loop_count=3,
      heating_temperature=37, heating_preheat=True,
      collect_beads_time_sec=3.0,
    )
    p.add_dry("Dry1", 300.0, TipPosition.AboveSurface)
    p.add_pause("Pause1", "")
    p.add_mix(
      "Mix2", None,
      precollect_enabled=True, loop_count=1,
      pause_tip_position=TipPosition.EdgeInLiquid,
      heating_temperature=32, heating_preheat=True,
      postmix_enabled=True, postmix_duration_sec=3.0,
      collect_beads_time_sec=5.0,
    )
    p.add_release_beads("ReleaseBeads2", "96 WP", 5.0, "Fast")
    bdz = p.to_bdz()
    assert bdz[:4] == bytes.fromhex("b6751cf2")
    _, _, _, exported = read_bdz(bdz)
    assert b"<ExportedData" in exported
    assert b"<Tip " in exported
    assert b"CollectBeads1" in exported
    assert b"ReleaseBeads1" in exported
    assert b"Mix1" in exported
    assert b"Dry1" in exported
    assert b"Pause1" in exported
    assert b"Mix2" in exported
    assert b"ReleaseBeads2" in exported
    assert len(p.plates) == 4
    assert len(p.tips) == 1
    assert len(p.tips[0].steps) == 7

  def test_roundtrip_96_well_bdz(self):
    """Parse 96-well example BDZ -> protocol -> to_bdz; re-parse and assert structure matches."""
    bdz_path = Path(__file__).resolve().parent / "presto_docs" / "260202_test-protocol-96.bdz"
    if not bdz_path.exists():
      pytest.skip("96-well example BDZ not found")
    bdz = bdz_path.read_bytes()
    p = parse_bdz_to_protocol(bdz)
    assert p.name == "260202_test-protocol-96"
    assert len(p.plates) == 4
    assert len(p.tips) == 1
    assert len(p.tips[0].steps) == 7
    out_bdz = p.to_bdz()
    assert len(out_bdz) > 0
    p2 = parse_bdz_to_protocol(out_bdz)
    assert p2.name == p.name
    assert len(p2.plates) == len(p.plates)
    assert len(p2.tips) == len(p.tips)
    assert len(p2.tips[0].steps) == len(p.tips[0].steps)
    plate_names = {pl.name for pl in p.plates}
    assert plate_names == {pl.name for pl in p2.plates}
    step_names = [s.name for s in p.tips[0].steps]
    assert step_names == [s.name for s in p2.tips[0].steps]


class TestPrestoHighLevelApi:
  """run_protocol uploads and starts with correct args; delegation to backend."""

  def _minimal_protocol(self) -> KingFisherProtocol:
    """One tip, one Mix step (for run_protocol tests)."""
    p = KingFisherProtocol(name="TestRun")
    tips_plate = Plate.create("Tips", PlateType.TIPS_96)
    p.add_tip(Tip(name="Tip1", plate=tips_plate, steps=[]))
    p.add_plate(Plate.create("96 DWP", PlateType.DWP_96))
    p.add_mix("Mix1", "96 DWP", image=Image.Heating, loop_count=3)
    return p

  def test_run_protocol_upload_and_start_with_protocol_name(self):
    mock_backend = MagicMock()
    mock_backend.upload_protocol = AsyncMock(return_value=None)
    mock_backend.start_protocol = AsyncMock(return_value=None)
    mock_backend._setup_finished = True
    presto = KingFisherPresto(backend=mock_backend)
    presto._setup_finished = True
    protocol = self._minimal_protocol()
    asyncio.run(presto.run_protocol(protocol))
    mock_backend.upload_protocol.assert_called_once()
    call_args = mock_backend.upload_protocol.call_args
    assert call_args[0][0] == protocol.name
    assert call_args[0][1] == protocol.to_bdz()
    mock_backend.start_protocol.assert_called_once_with(protocol.name, tip=None, step=None)

  def test_run_protocol_with_step_name_defaults_tip_for_single_tip(self):
    mock_backend = MagicMock()
    mock_backend.upload_protocol = AsyncMock(return_value=None)
    mock_backend.start_protocol = AsyncMock(return_value=None)
    mock_backend._setup_finished = True
    presto = KingFisherPresto(backend=mock_backend)
    presto._setup_finished = True
    protocol = self._minimal_protocol()
    asyncio.run(presto.run_protocol(protocol, step_name="Mix1"))
    mock_backend.start_protocol.assert_called_once_with(
      protocol.name, tip=protocol.tips[0].name, step="Mix1"
    )

  def test_run_protocol_then_next_event_loop_drains_events(self):
    """Canonical flow: run_protocol() then loop next_event() until Ready; events are received."""
    mock_backend = MagicMock()
    mock_backend.upload_protocol = AsyncMock(return_value=None)
    mock_backend.start_protocol = AsyncMock(return_value=None)
    mock_backend._setup_finished = True
    evt_ready = ET.fromstring('<Evt name="Ready"/>')
    mock_backend.get_event = AsyncMock(side_effect=[evt_ready])
    presto = KingFisherPresto(backend=mock_backend)
    presto._setup_finished = True
    protocol = self._minimal_protocol()

    async def run_then_drain():
      await presto.run_protocol(protocol)
      items = []
      while True:
        name, evt, ack = await presto.next_event()
        items.append((name, evt, ack))
        if name in ("Ready", "Aborted", "Error"):
          break
      return items

    items = asyncio.run(run_then_drain())
    assert len(items) == 1
    assert items[0][0] == "Ready"
    assert items[0][2] is None  # Ready has no ack callback

  def test_rotate_delegates_to_backend_with_position_and_location(self):
    mock_backend = MagicMock()
    mock_backend.rotate = AsyncMock(return_value=None)
    mock_backend._setup_finished = True
    presto = KingFisherPresto(backend=mock_backend)
    presto._setup_finished = True
    asyncio.run(presto.rotate(position=1, location=TurntableLocation.LOADING))
    mock_backend.rotate.assert_called_once_with(position=1, location=TurntableLocation.LOADING)

  def test_get_turntable_state_delegates_to_backend(self):
    mock_backend = MagicMock()
    mock_backend.get_turntable_state.return_value = {1: "processing", 2: "loading"}
    mock_backend._setup_finished = True
    presto = KingFisherPresto(backend=mock_backend)
    presto._setup_finished = True
    # get_turntable_state is async (due to @need_setup_finished decorator)
    result = asyncio.run(presto.get_turntable_state())
    assert result == {1: "processing", 2: "loading"}
    mock_backend.get_turntable_state.assert_called_once()

  def test_load_plate_delegates_to_backend(self):
    mock_backend = MagicMock()
    mock_backend.load_plate = AsyncMock(return_value=None)
    mock_backend._setup_finished = True
    presto = KingFisherPresto(backend=mock_backend)
    presto._setup_finished = True
    asyncio.run(presto.load_plate())
    mock_backend.load_plate.assert_called_once()

  def test_load_plate_when_backend_raises_value_error_propagates(self):
    mock_backend = MagicMock()
    mock_backend.load_plate = AsyncMock(side_effect=ValueError("Turntable state unknown; call rotate() first to establish state."))
    mock_backend._setup_finished = True
    presto = KingFisherPresto(backend=mock_backend)
    presto._setup_finished = True
    with pytest.raises(ValueError, match="Turntable state unknown"):
      asyncio.run(presto.load_plate())
