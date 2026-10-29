"""The chassis module: interlock, status display, deck I/O and the instrument's own sensors."""

from typing import Callable, Dict, List, Literal, Optional

from pylabrobot.beckman_coulter.biomek_i7.driver import protocol as p
from pylabrobot.beckman_coulter.biomek_i7.driver.board import AceBoard
from pylabrobot.beckman_coulter.biomek_i7.driver.configuration import ModuleConfiguration
from pylabrobot.beckman_coulter.biomek_i7.driver.features.module import (
  read_analog_inputs,
  read_module,
)

DoorState = Literal["open", "closed"]
DOOR_SENSOR_INPUT = "Door Sensor"
DOOR_CLOSED = 1


class Chassis:
  """The chassis's ACE module. Reached as `driver.chassis`."""

  def __init__(self, board: AceBoard):
    self.board = board
    self.configuration: Optional[ModuleConfiguration] = None

  async def discover(self) -> ModuleConfiguration:
    """Read what the chassis says about itself. Moves nothing."""
    self.configuration = await read_module(self.board)
    return self.configuration

  def _require_configuration(self) -> ModuleConfiguration:
    if self.configuration is None:
      raise RuntimeError("the chassis has not been read; call `discover()` or `setup()` first")
    return self.configuration

  async def read_analog_inputs(self) -> Dict[str, float]:
    """Every analog input (supply voltages, temperature...) by name, in its units."""
    return await read_analog_inputs(self.board, self._require_configuration())

  async def read_digital_inputs(self) -> Dict[str, int]:
    config = self._require_configuration()
    replies = await self.board.send_many(
      [p.read_input_output(p.Device.DIGITAL_INPUT, i) for i in config.digital_inputs]
    )
    return {config.digital_inputs[i]: r.payload[0] for i, r in zip(config.digital_inputs, replies)}

  async def read_digital_outputs(self) -> Dict[str, int]:
    config = self._require_configuration()
    replies = await self.board.send_many(
      [p.read_input_output(p.Device.DIGITAL_OUTPUT, i) for i in config.digital_outputs]
    )
    return {
      config.digital_outputs[i]: r.payload[0] for i, r in zip(config.digital_outputs, replies)
    }

  # -- door ------------------------------------------------------------------
  # The door sensor is the chassis's digital input "Door Sensor": 1 closed, 0 open (2026-07-31:
  # it read 0 when the door was opened, which tripped the interlock, and 1 again once shut).

  def _door_input(self) -> int:
    config = self._require_configuration()
    for index, name in config.digital_inputs.items():
      if name == DOOR_SENSOR_INPUT:
        return index
    raise RuntimeError(f"the chassis has no digital input named {DOOR_SENSOR_INPUT!r}")

  async def request_door_state(self) -> DoorState:
    """Whether the door is open or closed. Opening it trips the interlock. Moves nothing."""
    reply = await self.board.send(p.read_input_output(p.Device.DIGITAL_INPUT, self._door_input()))
    return "closed" if reply.payload[0] == DOOR_CLOSED else "open"

  def subscribe_door(self, callback: Callable[[DoorState], None]) -> Callable[[], None]:
    """Call `callback` whenever the door opens or closes. Returns an unsubscribe.

    The chassis reports changes only once asked to (Notify On Digital Input Change, which the
    Master Controller sends at every start and `notify_door_changes` sends)."""
    index = self._door_input()

    def on_message(reply: p.Reply) -> None:
      if (reply.device, reply.opcode) == p.DIGITAL_INPUT_VALUE and reply.index == index:
        callback("closed" if reply.payload[:1] == bytes([DOOR_CLOSED]) else "open")

    return self.board.subscribe(on_message)

  async def notify_door_changes(self, notify: bool = True) -> None:
    """Ask the chassis to report the door opening and closing. Moves nothing."""
    await self._unchecked_fw_notify_on_input_change(self._door_input(), notify)

  async def request_legacy_alp_devices(self) -> List[bytes]:
    """The legacy ALP devices the chassis lists, as the raw entries it answers."""
    payload = (await self.board.send(p.list_legacy_alp_devices())).payload
    return [payload[i : i + 2] for i in range(0, len(payload), 2)]

  # -- interlock -------------------------------------------------------------
  # The interlock cuts the arms' drive power when the light curtain or the door is broken, or the
  # host trips it; nothing moves while it is tripped. It can be read, tripped and re-armed; there is
  # deliberately no way here to bypass it.

  async def request_interlock_state(self) -> p.InterlockState:
    """`armed` (the drives may have power) or `tripped`. Moves nothing.

    The Master Controller never sends Get Interlock Status; it only reads the state.
    """
    return p.parse_interlock_status((await self.board.send(p.get_interlock_status())).payload)

  async def prepare_interlock(self) -> None:
    """Trip the interlock, ask to be told when it trips, and show READY, as before the motor tables
    are loaded after a power-up."""
    await self._unchecked_fw_trip_interlock()
    await self._unchecked_fw_notify_on_interlock_trip()
    await self._unchecked_fw_set_status_display("ready")

  async def _io_box_write_after_configure(self) -> None:
    """Send the digital I/O box the WRITE 01 the Master Controller sends after loading the motor
    tables. What it switches is not known, so it stays private and is replayed as sent."""
    await self._unchecked_fw_send_digital_io_box(b"\x02\x01")

  async def trip_interlock(self, error: bool = False) -> None:
    """Cut the arms' drive power and show READY: how the Master Controller leaves the instrument
    at rest (it does so at every start; the drive supply reads off in over 90% of its polls).
    With `error`, show ERROR, as it does after a failed move."""
    await self._unchecked_fw_trip_interlock()
    await self._unchecked_fw_set_status_display("error" if error else "ready")

  async def arm_interlock(self) -> None:
    """Re-arm the interlock and show RUNNING. The light curtain must be clear.

    Raises:
      RuntimeError: if the interlock does not report ARMED.
    """
    state = await self._unchecked_fw_rearm_interlock()
    if state[:1] != b"\x00":
      raise RuntimeError(
        f"the interlock did not arm (state {state.hex()}); is the light curtain clear?"
      )
    await self._unchecked_fw_set_status_display("running")

  # -- firmware commands -----------------------------------------------------
  # The chassis's writes, one request each, as the Master Controller sends them. No checks and no
  # bookkeeping: the public methods above decide when each is sent.

  async def _unchecked_fw_trip_interlock(self) -> None:
    await self.board.send(p.trip_interlock())

  async def _unchecked_fw_rearm_interlock(self) -> bytes:
    """Re-Arm Interlock. Returns what it answers: `00` when armed."""
    return (await self.board.send(p.rearm_interlock())).payload

  async def _unchecked_fw_notify_on_interlock_trip(self) -> None:
    await self.board.send(p.notify_on_interlock_trip())

  async def _unchecked_fw_set_status_display(self, state: p.StatusDisplayState) -> None:
    await self.board.send(p.set_status_display(state))

  async def _unchecked_fw_send_digital_io_box(self, data: bytes) -> None:
    await self.board.send(p.send_digital_io_box(data))

  async def _unchecked_fw_notify_on_input_change(self, index: int, notify: bool) -> None:
    await self.board.send(p.notify_on_change(p.Device.DIGITAL_INPUT, index, notify))
