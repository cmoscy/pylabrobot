# KingFisher Presto

The **KingFisher Presto** is a magnetic particle processor for sample prep. This package controls it over USB HID using the instrument’s XML command/response/event protocol.

## Requirements

- KingFisher Presto connected via USB (VID `0x0AB6`, PID `0x02C9` per Interface Specification)
- Python dependency: `hidapi` (e.g. `pip install hidapi`)

## Connecting

```python
import asyncio
from pylabrobot.particle_processing.kingfisher import KingFisherPresto, KingFisherPrestoBackend

async def main():
    backend = KingFisherPrestoBackend()  # optional: serial_number="...", vid=..., pid=...
    presto = KingFisherPresto(backend=backend)

    async with presto:
        await presto.setup()
        # presto.instrument, presto.version, presto.serial are now set
        # ... run commands and event loop ...
    # stop() and Disconnect are called on exit

asyncio.run(main())
```

Use `KingFisherPrestoBackend(serial_number="12345")` when multiple Presto units are attached.

## Commands (high-level)

All of these require the machine to be set up (e.g. inside `async with presto` after `await presto.setup()`).

| Method | Description |
|--------|-------------|
| `await presto.get_status()` | Returns `dict` with `ok`, `status` ("Idle" / "Busy" / "In error"), `error_code`, `error_text`, `error_code_description`. |
| `await presto.list_protocols()` | Returns `(list of protocol names, memory_used_percent)`. |
| `await presto.download_protocol(name)` | Download protocol `name` from instrument; returns raw bytes. |
| `await presto.upload_protocol(name, protocol_bytes, crc=None)` | Upload a protocol; `crc` is optional (computed from bytes if omitted). |
| `await presto.start_protocol(protocol, tip=None, step=None)` | Start full protocol or a single tip/step. Protocol must already be in instrument memory. |
| `await presto.stop_protocol()` | Stop current run. |
| `await presto.acknowledge()` | Required after **LoadPlate**, **RemovePlate**, **ChangePlate**, or **Pause** events before the instrument continues. |
| `await presto.error_acknowledge()` | Clear instrument error state (e.g. after an **Error** event). |
| `await presto.abort()` | Two-phase abort: stops execution and flushes buffers. Use for immediate halt. |

## Running a protocol and handling events

Execution is **event-driven**. After you start a protocol, the instrument sends events (e.g. **LoadPlate**, **RemovePlate**, **ChangePlate**, **Pause**). You must **acknowledge** when required so the run can proceed.

1. **Start the protocol** (must already be in instrument memory, e.g. uploaded via BindIt or `upload_protocol`):

   ```python
   await presto.start_protocol("My Protocol")
   ```

2. **Process events in a loop** and call `acknowledge()` or `error_acknowledge()` when needed:

   ```python
   async for evt in presto.events():
       name = evt.get("name")
       if name == "LoadPlate":
           plate = evt.get("plate", "?")
           print(f"Load plate: {plate}")
           # ... perform load (e.g. prompt user or robot) ...
           await presto.acknowledge()
       elif name == "RemovePlate":
           plate = evt.get("plate", "?")
           print(f"Remove plate: {plate}")
           # ... perform remove ...
           await presto.acknowledge()
       elif name == "ChangePlate":
           # Nested Evt children may describe RemovePlate / LoadPlate
           await presto.acknowledge()
       elif name == "Pause":
           await presto.acknowledge()
       elif name == "Error":
           err = evt.find("Error")
           code = err.get("code") if err is not None else None
           text = (err.text or "").strip() if err is not None else ""
           print(f"Error {code}: {text}")
           await presto.error_acknowledge()
       elif name == "Ready":
           print("Protocol finished.")
           break
       elif name == "Aborted":
           print("Run aborted.")
           break
   ```

3. **Optional: run protocol and event loop concurrently** with other async work (e.g. other instruments) by scheduling the event loop as a task and using `get_event()` or the same `async for evt in presto.events()` pattern from a single coroutine.

Events are XML elements; use `evt.get("name")`, `evt.get("plate")`, `evt.find("Error")`, etc. See the KingFisher Presto Interface Specification for the full list of events and attributes.

## Error handling

- **PrestoConnectionError**: Raised when a command returns `ok="false"` or on timeout. The exception has `message`, `code`, and `res_name`; message includes standard error code descriptions when available.
- **get_status()** returns `error_code`, `error_text` (instrument message), and `error_code_description` (standard description from the spec) so you can inspect errors without raising.

## Low-level / backend

- **KingFisherPrestoBackend** can be used without the **KingFisherPresto** frontend if you prefer; it exposes the same async methods. The frontend adds `@need_setup_finished` checks and the standard Machine lifecycle.
- **PrestoConnection** (used by the backend) is not part of the public API; the backend sends Connect in `setup()` and Disconnect in `stop()`.