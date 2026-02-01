"""Unit tests for KingFisher Presto connection and backend helpers.

Tests _find_complete_message (XML message boundaries, nested Res/Evt) and
XML command building (_cmd_xml) per the KingFisher Presto Interface Specification.
"""


from .presto_connection import _find_complete_message
from .presto_backend import _cmd_xml


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
