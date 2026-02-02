# KingFisher Presto

Control the **KingFisher Presto** magnetic particle processor from Python over USB HID using the instrument’s XML command/response/event protocol. This document gives a high-level overview, connection lifecycle, PLR command examples, protocol management, and how to run multi-step BindIt protocols with event handling.

---

## 1. High-level overview and control summary

| Layer | Role |
|-------|------|
| **KingFisherPresto** (frontend) | High-level API: setup/stop, status, turntable, single-step commands (mix, dry, pause, etc.), protocol start/upload/download, event handling. Enforces “setup finished” and Machine lifecycle. |
| **KingFisherPrestoBackend** | Same async operations; no lifecycle checks. Use directly if you prefer. |
| **Connection** | USB HID (VID `0x0AB6`, PID `0x02C9`). XML Cmd/Res/Evt over 64-byte reports. One command in flight; events queued. |

**Control modes**

- **Direct commands** — Turntable **Rotate** (no protocol). Backend waits for Ready/Error.
- **Single-step PLR commands** — Build a minimal .bdz, upload to a slot, start, wait for Ready (e.g. `mix()`, `dry()`, `pause()`).
- **Multi-step BindIt protocols** — Upload or use existing protocol; `start_protocol(name)` then handle **LoadPlate**, **RemovePlate**, **ChangePlate**, **Pause** by calling **Acknowledge** so the run continues.

**Requirements:** KingFisher Presto on USB; Python `hidapi` (e.g. `pip install hidapi`). See KingFisher Presto Interface Specification for VID/PID and protocol details.

---

## 2. Connection and lifecycle management

### Setup and teardown

- **Setup:** Open HID, send **Connect**, parse instrument/version/serial, start the background read loop. All high-level commands require setup to have completed (e.g. after `await presto.setup()` inside `async with presto` or similar).
- **Planned:** Ethernet Connection management option
- **Stop:** Send **Disconnect**, stop the read loop, close HID. Instrument may not reply to Disconnect if already closed.
- **Idempotent setup:** Calling `setup()` again while already connected does **not** re-open the device (which would raise `HIDException`). The connection layer returns immediately; the backend still re-sends Connect, resets turntable state to unknown, and refreshes instrument/version/serial. Use this to “re-verify” connection and state without disconnecting.

### Assumptions during setup

- **Turntable state** — Reset to **unknown** at the start of every `setup()` (and cleared again in `stop()`). So after connect or reconnect we do not assume which position (1 or 2) is at processing or loading until you run a successful **Rotate** (or use `initialize_turntable=True`; see below).
- **`initialize_turntable`** — Optional: `await presto.setup(initialize_turntable=True)`. After Connect, the backend runs `rotate(position=1, location="processing")` so the turntable is in a known power-on state (position 1 at processing, position 2 at loading). The table may move on connect. Default is `False` so we do not move the table unless you opt in.
- **Run state on connect** — The frontend calls `get_run_state()` after setup and warns if the instrument is not Idle (e.g. Busy or In error). If Busy, you can attach with `continue_run()`.


### Example: connect and disconnect

```python
import asyncio
from pylabrobot.particle_processing.kingfisher import KingFisherPresto, KingFisherPrestoBackend

async def main():
    backend = KingFisherPrestoBackend()  # optional: serial_number="...", vid=..., pid=...
    presto = KingFisherPresto(backend=backend)

    async with presto:
        await presto.setup()  # or setup(initialize_turntable=True)
        # presto.instrument, presto.version, presto.serial are set
        # ... run commands ...
    # stop() and Disconnect on exit

asyncio.run(main())
```

Use `KingFisherPrestoBackend(serial_number="...")` when multiple Presto units are attached.

---

## 3. PLR command examples

All of these require the machine to be set up. Commands are async; use `await` in async code (e.g. Jupyter, or an async script).

### Turntable (direct commands)

The turntable has two **positions** (slots) 1 and 2. Each can be at **processing** (under the magnetic head) or **loading** (load/unload station). State is inferred only from **Rotate** commands that complete with Ready; it is unknown after setup/stop until the first successful rotate (or `setup(initialize_turntable=True)`).

| Method | Description |
|--------|-------------|
| `await presto.rotate(position=1, location=...)` | Move position 1 or 2 to `"processing"` or `"loading"`. Use `TurntableLocation.PROCESSING` / `TurntableLocation.LOADING` or strings. Blocks until Ready/Error. |
| `await presto.get_turntable_state()` | Returns `{1: "processing"\|"loading"\|None, 2: ...}`. |
| `await presto.load_plate()` | Convenience: rotate so whatever is at loading moves to processing. Requires known turntable state (call `rotate()` first or `setup(initialize_turntable=True)`). |

**Turntable positioning:**

```python
from pylabrobot.particle_processing.kingfisher import KingFisherPresto, KingFisherPrestoBackend, TurntableLocation

# after await presto.setup()
await presto.rotate(position=1, location=TurntableLocation.LOADING)
print(await presto.get_turntable_state())  # e.g. {1: "loading", 2: "processing"}

await presto.rotate(position=1, location="processing")
await presto.load_plate()  # if state known: bring plate at loading to processing
```

### Single-step commands (WIP)

Each builds a minimal .bdz, uploads to a slot (e.g. `plr_Mix`, `plr_Pause`), starts the protocol, and optionally waits for Ready.

| Method | Description |
|--------|-------------|
| `await presto.pause(message="...")` | Single Pause step. |
| `await presto.mix(plate, duration_sec, speed="Medium"\|"Fast", ...)` | Single Mix step. Speeds: Medium, Fast. |
| `await presto.dry(duration_sec, plate="Plate1", tip_position="AboveSurface", ...)` | Single Dry step. |
| `await presto.collect_beads(count=3, collect_time_sec=30, plate="Plate1", ...)` | Single CollectBeads step (count 1..5). |
| `await presto.release_beads(duration_sec, speed="Fast", plate="Plate1", ...)` | Single ReleaseBeads step. |

**Sync (block until Ready):** Default `wait_until_ready=True`. The call returns when the step has completed.

```python
await presto.pause(message="Demo")
await presto.mix("Plate1", 10.0, speed="Medium")
await presto.dry(30.0, plate="Plate1")
```

**Async (fire-and-forget):** Set `wait_until_ready=False`; the call returns as soon as the step is started. Handle events (e.g. LoadPlate, Ready) yourself via `run_until_ready()` or `events()`.

```python
await presto.mix("Wash1", 5.0, wait_until_ready=False)
# later: async for name, evt, ack in presto.run_until_ready():
#     if ack: await ack()
#     if name in ("Ready", "Error"): break
```

### Single-step commands (planned)

| Method | Status | Workaround |
|--------|--------|------------|
| `await presto.pick_up_tips()` | Not implemented (raises `NotImplementedError`) | Use `start_protocol(protocol, tip=..., step=...)` with a protocol that has a tip pickup step. |
| `await presto.drop_tips()` | Not implemented (raises `NotImplementedError`) | Use `start_protocol(protocol, tip=..., step=...)` with a protocol that has a drop-tips step. |

---

## 4. Protocol management (list, upload, download)

Protocols live in instrument memory. You can list, upload (e.g. from file or from our BDZ builders), and download them.

| Method | Description |
|--------|-------------|
| `await presto.list_protocols()` | Returns `(list of protocol names, memory_used_percent)`. |
| `await presto.upload_protocol(name, protocol_bytes, crc=None)` | Upload a protocol; `crc` optional (computed from bytes if omitted). |
| `await presto.download_protocol(name)` | Download protocol `name` from instrument; returns raw bytes. |

**Example: list and upload**

```python
names, mem = await presto.list_protocols()
print("Protocols:", names, "Memory used %:", mem)

# Upload a .bdz you built or read from disk
with open("my_protocol.bdz", "rb") as f:
    raw = f.read()
await presto.upload_protocol("MyProtocol", raw)
```

**Example: download and save**

```python
raw = await presto.download_protocol("MyProtocol")
with open("backup.bdz", "wb") as f:
    f.write(raw)
```

Single-step PLR commands (mix, dry, etc.) upload to fixed slot names (e.g. `plr_Mix`, `plr_Pause`) and then start that protocol; they overwrite any existing protocol with that name.

---

## 5. Multi-step BindIt protocols: run and control with Acknowledge and events

Protocols authored in BindIt (or uploaded as .bdz) can have multiple steps and require **Acknowledge** for **LoadPlate**, **RemovePlate**, **ChangePlate**, and **Pause** events. You run them with `start_protocol()` and drive progress by consuming the event stream and calling `acknowledge()` (or `error_acknowledge()` on **Error**).

### Start a full protocol or a single tip/step

- **Full protocol:** `await presto.start_protocol("My Protocol")`
- **Single tip/step:** `await presto.start_protocol("My Protocol", tip="Tip1", step="Step1")`
  The protocol must already be in instrument memory (e.g. uploaded via BindIt or `upload_protocol`).

### Event stream and Acknowledge

After starting, the instrument sends events. You must **acknowledge** when required or the run will not advance.

| Event | Action |
|-------|--------|
| **LoadPlate**, **RemovePlate**, **ChangePlate**, **Pause** | Call `await presto.acknowledge()` so the instrument continues. |
| **Error** | Call `await presto.error_acknowledge()`; inspect `evt.find("Error")` for code/text. |
| **Ready** | Run completed successfully. |
| **Aborted** | Run aborted. |

**Option A — Async iterator over events (recommended):**

```python
await presto.start_protocol("My Protocol")

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
        await presto.acknowledge()
    elif name in ("ChangePlate", "Pause"):
        await presto.acknowledge()
    elif name == "Error":
        err = evt.find("Error")
        code = err.get("code") if err is not None else None
        text = (err.text or "").strip() if err is not None else ""
        print(f"Error {code}: {text}")
        await presto.error_acknowledge()
        break
    elif name == "Ready":
        print("Protocol finished.")
        break
    elif name == "Aborted":
        print("Run aborted.")
        break
```

**Option B — Helper generator `run_until_ready()` (name, evt, ack callback):**

```python
await presto.start_protocol("My Protocol")

async for name, evt, ack in presto.run_until_ready():
    if ack is not None:
        await ack()
    if name in ("Ready", "Aborted", "Error"):
        break
```

**Option C — Attach to an already-running protocol (`continue_run()`):**

If after `setup()` the instrument is **Busy**, use `continue_run()` to attach to the same event stream (e.g. after a restart or re-setup):

```python
async for name, evt, ack in presto.continue_run():
    if ack is not None:
        await ack()
    if name in ("Ready", "Aborted", "Error"):
        break
```

Events are XML elements; use `evt.get("name")`, `evt.get("plate")`, `evt.find("Error")`, etc. See the KingFisher Presto Interface Specification for the full event list and attributes.

---

## Error handling

- **PrestoConnectionError** — Raised when a command returns `ok="false"` or on timeout. Attributes: `message`, `code`, `res_name`. Message includes standard error code descriptions when available.
- **get_status()** — Returns `error_code`, `error_text`, `error_code_description` so you can inspect errors without raising.
- **rotate()** — On **Error** event, state is not updated; backend calls `error_acknowledge()` and raises `PrestoConnectionError`.

---

## Step-by-step notebook

[**kingfisher_presto_step_by_step.ipynb**](kingfisher_presto_step_by_step.ipynb) walks through connect, status, turntable, single-step commands, and tip pickup/drop (via protocol step) in separate cells.

---

## Low-level / backend

- **KingFisherPrestoBackend** exposes the same async operations without frontend lifecycle checks; use it directly if you prefer.
- **PrestoConnection** (used by the backend) is not part of the public API; the backend sends Connect in `setup()` and Disconnect in `stop()`.
