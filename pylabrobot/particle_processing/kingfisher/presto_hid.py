"""KingFisher Presto HID transport: extends pylabrobot HID with send_feature_report.

The KingFisher Presto uses a 2-byte Feature report on the control endpoint for
Abort and flow control (Interface Specification 3.2.3, 3.2.4). Rather than
extending the generic io.hid API, we subclass HID here and add the method
only for this device.
"""

import asyncio

from pylabrobot.io.hid import HID


class KingFisherHID(HID):
  """HID transport for KingFisher Presto: adds send_feature_report for Abort/flow control."""

  async def send_feature_report(self, data: bytes) -> int:
    """Send a Feature report via the control endpoint.

    KingFisher Presto uses this for Abort (first byte nonzero, second zero)
    and optional flow control (first byte 0, second nonzero to pause; both 0 to resume).
    See Interface Specification 3.2.3, 3.2.4.

    Args:
      data: Full report data (e.g. 2 bytes for KingFisher). Report ID is not prepended.

    Returns:
      Number of bytes written.
    """
    loop = asyncio.get_running_loop()

    def _send():
      assert self.device is not None, "Call setup() first."
      return self.device.send_feature_report(list(data))

    if self._executor is None:
      raise RuntimeError("Call setup() first.")
    return await loop.run_in_executor(self._executor, _send)
