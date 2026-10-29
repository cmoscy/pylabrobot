"""USB transfer boundaries tested without opening a hardware connection."""

import unittest
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import Mock, patch

from pylabrobot.io import usb as usb_module
from pylabrobot.io.usb import USB


class USBReadTests(unittest.IsolatedAsyncioTestCase):
  """Keep USB transfer completion separate from individual endpoint packets."""

  async def asyncSetUp(self) -> None:
    """Attach a mock device and an executor without device discovery."""
    backend = SimpleNamespace(
      util=SimpleNamespace(find_descriptor=Mock()), core=SimpleNamespace(USBError=OSError)
    )
    patcher = patch.object(usb_module, "usb", backend, create=True)
    patcher.start()
    self.addCleanup(patcher.stop)
    self.io = USB(1, 2, "mock USB")
    self.device = Mock()
    self.io.dev = self.device
    self.io.read_endpoint = Mock(wMaxPacketSize=64, bEndpointAddress=0x81)
    self.io._read_executor = ThreadPoolExecutor(max_workers=1)

  async def asyncTearDown(self) -> None:
    """Release the fixture worker after all reads finish."""
    self.io._shutdown_executors()

  async def test_packet_aligned_reply_does_not_consume_next_reply(self) -> None:
    """A 64-byte result from a larger request has already consumed its ZLP."""
    self.device.read.side_effect = [b"a" * 64, b"next reply"]
    self.assertEqual(await self.io.read(), b"a" * 64)
    self.device.read.assert_called_once_with(self.io.read_endpoint, 4032, timeout=3000)
    self.assertEqual(await self.io.read(), b"next reply")

  async def test_multiple_packets_fit_one_transfer(self) -> None:
    """Read the complete 384-byte reply and its terminator in one request."""
    self.device.read.return_value = b"a" * 384
    self.assertEqual(await self.io.read(), b"a" * 384)
    self.device.read.assert_called_once_with(self.io.read_endpoint, 4032, timeout=3000)

  async def test_full_transfer_continues_until_short_transfer(self) -> None:
    """A filled buffer is not a response boundary."""
    self.device.read.side_effect = [b"a" * 4032, b"b" * 65]
    self.assertEqual(await self.io.read(), b"a" * 4032 + b"b" * 65)
    self.assertEqual([call.args[1] for call in self.device.read.call_args_list], [4032, 4032])

  async def test_full_transfer_followed_by_zlp(self) -> None:
    """Preserve a response that exactly fills the host buffer."""
    self.device.read.side_effect = [b"a" * 4032, b"", b"next reply"]
    self.assertEqual(await self.io.read(), b"a" * 4032)
    self.assertEqual(self.device.read.call_count, 2)
    self.assertEqual(await self.io.read(), b"next reply")

  async def test_high_speed_endpoint_uses_larger_default_buffer(self) -> None:
    """A 512-byte endpoint uses a packet-aligned buffer without combining responses."""
    self.io.read_endpoint = Mock(wMaxPacketSize=512, bEndpointAddress=0x81)
    self.device.read.side_effect = [b"a" * 512, b"next reply"]
    self.assertEqual(await self.io.read(), b"a" * 512)
    self.device.read.assert_called_once_with(self.io.read_endpoint, 4096, timeout=3000)
    self.assertEqual(await self.io.read(), b"next reply")

  async def test_explicit_size_uses_transfer_length_for_completion(self) -> None:
    """A short transfer must not concatenate the next response to a packet-sized reply."""
    self.device.read.side_effect = [b"a" * 64, b"next reply"]
    self.assertEqual(await self.io.read(size=512), b"a" * 64)
    self.assertEqual(self.device.read.call_count, 1)
    self.assertEqual(await self.io.read(), b"next reply")

  async def test_explicit_size_is_an_upper_bound(self) -> None:
    """An explicitly filled byte limit returns without starting another read."""
    self.device.read.side_effect = [b"a" * 128, b"next reply"]
    self.assertEqual(await self.io.read(size=128), b"a" * 128)
    self.device.read.assert_called_once_with(self.io.read_endpoint, 128, timeout=3000)

  async def test_empty_completion_is_not_an_empty_response(self) -> None:
    """Skip an empty completion before a response has started."""
    self.device.read.side_effect = [b"", b"reply"]
    self.assertEqual(await self.io.read(), b"reply")

  async def test_alternate_endpoint_determines_buffer_alignment(self) -> None:
    """Round the requested buffer for the selected endpoint's packet size."""
    endpoint = Mock(wMaxPacketSize=512, bEndpointAddress=0x83)
    self.device.get_active_configuration.return_value = {(0, 0): [endpoint]}
    self.device.read.return_value = b"reply"
    with patch("pylabrobot.io.usb.usb.util.find_descriptor", return_value=endpoint):
      self.assertEqual(await self.io.read(endpoint=0x83), b"reply")
    self.device.read.assert_called_once_with(0x83, 4096, timeout=3000)

  async def test_drain_uses_default_transfer_size(self) -> None:
    """Draining uses the same buffer and continues until the endpoint times out."""
    self.device.read.side_effect = [b"stale reply", b"a" * 384, OSError("timeout")]
    await self.io.drain()
    self.assertEqual([call.args[1] for call in self.device.read.call_args_list], [4032] * 3)

  async def test_drain_respects_explicit_transfer_size(self) -> None:
    """An explicit drain buffer size overrides the shared default."""
    self.device.read.side_effect = [b"stale reply", OSError("timeout")]
    await self.io.drain(size=128)
    self.assertEqual([call.args[1] for call in self.device.read.call_args_list], [128, 128])


class USBWriteTests(unittest.IsolatedAsyncioTestCase):
  """End a packet-aligned write with a zero-length packet only when the device expects one."""

  def _make_io(self, **kwargs) -> USB:
    """A USB object attached to a mock device with a 32-byte write endpoint."""
    backend = SimpleNamespace(
      util=SimpleNamespace(find_descriptor=Mock()), core=SimpleNamespace(USBError=OSError)
    )
    patcher = patch.object(usb_module, "usb", backend, create=True)
    patcher.start()
    self.addCleanup(patcher.stop)
    io = USB(1, 2, "mock USB", **kwargs)
    io.dev = Mock()
    io.read_endpoint = Mock(wMaxPacketSize=16, bEndpointAddress=0x81)
    io.write_endpoint = Mock(wMaxPacketSize=32, bEndpointAddress=0x02)
    io._read_executor = ThreadPoolExecutor(max_workers=1)
    io._write_executor = ThreadPoolExecutor(max_workers=1)
    self.addCleanup(io._shutdown_executors)
    return io

  async def test_packet_aligned_write_sends_zero_length_packet_by_default(self) -> None:
    """A write that fills whole packets is followed by an empty one."""
    io = self._make_io()
    await io.write(b"a" * 32)
    self.assertEqual([call.args[1] for call in io.dev.write.call_args_list], [b"a" * 32, b""])

  async def test_packet_aligned_write_without_zero_length_packet(self) -> None:
    """A device that delimits its own messages gets the data alone."""
    io = self._make_io(send_zero_length_packet=False)
    await io.write(b"a" * 32)
    self.assertEqual([call.args[1] for call in io.dev.write.call_args_list], [b"a" * 32])

  async def test_short_write_never_sends_zero_length_packet(self) -> None:
    """A short packet already ends the transfer."""
    io = self._make_io()
    await io.write(b"a" * 7)
    self.assertEqual([call.args[1] for call in io.dev.write.call_args_list], [b"a" * 7])

  def test_serialize_records_only_a_disabled_zero_length_packet(self) -> None:
    """The default is left out, so existing serialized devices are unchanged."""
    self.assertNotIn("send_zero_length_packet", self._make_io().serialize())
    self.assertFalse(
      self._make_io(send_zero_length_packet=False).serialize()["send_zero_length_packet"]
    )


class USBDeviceSelectionTests(unittest.TestCase):
  """Pick a device by serial number, reading the descriptor rather than PyUSB's empty cache."""

  def _devices(self, *serials):
    devices = []
    for serial in serials:
      dev = Mock(iSerialNumber=3 if serial else 0, _serial_number=None, serial_number=serial)
      devices.append(dev)
    return devices

  def test_serial_number_is_read_through_the_descriptor(self) -> None:
    devices = self._devices("A", "B")
    with patch.object(usb_module, "libusb_package", create=True) as lib:
      lib.find.return_value = devices
      self.assertEqual(
        USB(1, 2, "mock USB", serial_number="B").get_available_devices(), [devices[1]]
      )

  def test_device_without_serial_number_is_refused(self) -> None:
    with patch.object(usb_module, "libusb_package", create=True) as lib:
      lib.find.return_value = self._devices(None)
      with self.assertRaisesRegex(RuntimeError, "does not have a serial number"):
        USB(1, 2, "mock USB", serial_number="B").get_available_devices()
