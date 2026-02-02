"""Build minimal BindIt .bdz protocol files for single-step KingFisher Presto protocols.

Used by the liquid-handler-like API (mix, dry, collect_beads, release_beads, pause).
Only supported speed names are exposed; unsupported speeds raise ValueError.
See BDZ_FORMAT.md for layout and step parameters.
"""

import gzip
import struct
import uuid
from typing import Optional

# Protocol slot names for dynamic step-wise execution (overwrite by default).
STEP_SLOTS = {
  "Mix": "plr_Mix",
  "Dry": "plr_Dry",
  "CollectBeads": "plr_CollectBeads",
  "ReleaseBeads": "plr_ReleaseBeads",
  "Pause": "plr_Pause",
}

# Supported mix speeds (BindIt UI name -> GUID). Unsupported: Slow, Bottom mix, Half mix.
# From BDZ_FORMAT.md / Purification_Zymo.bdz.
SPEED_GUIDS = {
  "Medium": "2e7c9f99-d2c0-4baf-b04c-979e0ee3de00",
  "Fast": "6e89445e-98b2-43c5-8ae5-c37ed517f506",
}
# Postmix default = Medium; use this GUID for Postmix <Speed> when required.
POSTMIX_SPEED_GUID = "563b24fa-2eb7-4497-928b-5e91b740a01e"

# Unsupported speeds (no confirmed GUID in our sample). Do not guess.
UNSUPPORTED_SPEEDS = frozenset({"Slow", "Bottom mix", "Half mix"})

# KingFisher Presto InstrumentTypeId (BindIt).
INSTRUMENT_TYPE_ID = "9da3c7a3-bfb4-455e-b1c6-86f668e44ed0"

# BDZ header: magic (4) + version/flags (8) + length (4) + str_len (2) + "BindIt Software" (15)
# + version_str_len (2) + "4.0.0.45" (8) + reserved (17) = 61 bytes.
BDZ_MAGIC = bytes.fromhex("b6751cf2")
BDZ_VERSION_FLAGS = bytes.fromhex("01000a0001000000")
BDZ_LENGTH = 27
BDZ_STRING_LEN_15 = 15
BDZ_BINDIT_SOFTWARE = b"BindIt Software"
BDZ_VERSION_STR_LEN = 8
BDZ_VERSION_STR = b"4.0.0.45"
# Reserved bytes 44-60 (17 bytes). Use fixed pattern from sample; may need validation on instrument.
BDZ_RESERVED = bytes.fromhex("0b00010000000000390a00") + b"\x00" * 7

# Deterministic GUID namespace for plates/steps (same inputs -> same .bdz for tests).
_NAMESPACE_BDZ = uuid.UUID("7c9e6679-7425-40de-944b-e07fc1f90ae7")


def _guid(s: str) -> str:
  """Deterministic GUID from string."""
  return str(uuid.uuid5(_NAMESPACE_BDZ, s))


def _check_speed(speed: str) -> None:
  if speed in UNSUPPORTED_SPEEDS:
    raise ValueError(
      f"Speed {speed!r} is not supported (no confirmed GUID). "
      f"Supported: {sorted(SPEED_GUIDS)}. Unsupported: {sorted(UNSUPPORTED_SPEEDS)}."
    )
  if speed not in SPEED_GUIDS:
    raise ValueError(
      f"Unknown speed {speed!r}. Supported: {sorted(SPEED_GUIDS)}."
    )


def _duration_sec_to_xml(seconds: float) -> str:
  """Convert seconds to XML Schema duration (e.g. PT30S, PT5M)."""
  s = int(round(seconds))
  if s < 60:
    return f"PT{s}S"
  m, s = divmod(s, 60)
  if s == 0:
    return f"PT{m}M"
  return f"PT{m}M{s}S"


def _build_header() -> bytes:
  """Build 61-byte BDZ header."""
  return (
    BDZ_MAGIC
    + BDZ_VERSION_FLAGS
    + struct.pack("<I", BDZ_LENGTH)
    + struct.pack("<H", BDZ_STRING_LEN_15)
    + BDZ_BINDIT_SOFTWARE
    + struct.pack("<H", BDZ_VERSION_STR_LEN)
    + BDZ_VERSION_STR
    + BDZ_RESERVED
  )


def _build_properties_xml(protocol_name: str) -> str:
  """Minimal Properties XML (metadata)."""
  obj_id = _guid(f"properties:{protocol_name}")
  return f"""<?xml version="1.0" encoding="utf-8"?>
<Properties version="1">
  <ExportedObject name="{protocol_name}" id="{obj_id}">
    <InstrumentTypeId>{INSTRUMENT_TYPE_ID}</InstrumentTypeId>
    <CreatorName>pylabrobot</CreatorName>
    <Timestamp>0001-01-01T00:00:00</Timestamp>
    <ExecutionTime>0001-01-01T00:00:00</ExecutionTime>
  </ExportedObject>
  <Flags><FactoryData>false</FactoryData></Flags>
  <InstrumentParameters type="713"><ProtocolType>1</ProtocolType></InstrumentParameters>
</Properties>"""


def _build_exported_data_xml(
  protocol_name: str,
  tip_plate_name: str,
  tip_plate_id: str,
  step_plate_name: str,
  step_plate_id: str,
  step_xml: str,
  step_name: str,
) -> str:
  """Build ExportedData XML: PlateLayout (2 plates) + Run + Protocol + Tip + one step."""
  run_id = _guid(f"run:{protocol_name}")
  protocol_id = _guid(f"protocol:{protocol_name}")
  tip_id = _guid(f"tip:Tip1:{protocol_name}")
  tip_persistent_id = _guid(f"tip_persistent:Tip1:{protocol_name}")

  return f"""<?xml version="1.0" encoding="utf-8"?>
<ExportedData xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
  <Protocol>
    <Run ID="{run_id}">
      <PlateLayout>
        <Plates>
          <Plate id="{tip_plate_id}" name="{tip_plate_name}" plateTypeID="{_guid('plateType:DWP')}"/>
          <Plate id="{step_plate_id}" name="{step_plate_name}" plateTypeID="{_guid('plateType:plate')}"/>
        </Plates>
      </PlateLayout>
      <Protocol name="{protocol_name}" ID="{protocol_id}" locked="false" IsExecutable="true">
        <Containers/>
        <Steps>
          <Tip name="Tip1" id="{tip_id}" persistentID="{tip_persistent_id}" enabled="true">
            <Plates>
              <Plate id="{tip_plate_id}" wellGroup="Plate"/>
              <Plate id="{tip_plate_id}" wellGroup="Plate"/>
            </Plates>
            <Steps>
              {step_xml}
            </Steps>
          </Tip>
        </Steps>
      </Protocol>
    </Run>
  </Protocol>
</ExportedData>"""


def _mix_step_xml(
  step_name: str,
  plate_id: str,
  duration_sec: float,
  speed: str,
  image: str,
  loop_count: int,
) -> str:
  """One Mix step XML (required elements per BDZ_FORMAT)."""
  _check_speed(speed)
  speed_guid = SPEED_GUIDS[speed]
  duration = _duration_sec_to_xml(duration_sec)
  return f"""<Mix name="{step_name}" enabled="true">
                <Image>{image}</Image>
                <Precollect enabled="false"/>
                <ReleaseBeads enabled="false"><Duration>PT0S</Duration><Speed>{speed_guid}</Speed></ReleaseBeads>
                <Mixing><Shakes><Shake duration="{duration}" speed="{speed_guid}"/></Shakes><LoopCount>{loop_count}</LoopCount><PauseTipPosition>AboveSurface</PauseTipPosition></Mixing>
                <Pause enabled="false"><Message/></Pause>
                <Heating enabled="false"><Temperature>0</Temperature><Preheat>false</Preheat></Heating>
                <Postmix enabled="false"><Duration>PT0S</Duration><Speed>{POSTMIX_SPEED_GUID}</Speed></Postmix>
                <CollectBeads enabled="true"><Count>3</Count><CollectTime>PT30S</CollectTime></CollectBeads>
                <PostTemperature enabled="false"><Temperature>0</Temperature></PostTemperature>
                <Plates><Plate id="{plate_id}" wellGroup="Plate"/></Plates>
              </Mix>"""


def _dry_step_xml(
  step_name: str,
  plate_id: str,
  duration_sec: float,
  tip_position: str,
) -> str:
  """One Dry step XML."""
  duration = _duration_sec_to_xml(duration_sec)
  return f"""<Dry name="{step_name}" enabled="true">
                <Duration>{duration}</Duration>
                <TipPosition>{tip_position}</TipPosition>
                <Plates><Plate id="{plate_id}" wellGroup="Plate"/></Plates>
              </Dry>"""


def _collect_beads_step_xml(
  step_name: str,
  plate_id: str,
  count: int,
  collect_time_sec: float,
) -> str:
  """One CollectBeads step XML (standalone)."""
  if not 1 <= count <= 5:
    raise ValueError("Collect beads count must be 1..5")
  collect_time = _duration_sec_to_xml(collect_time_sec)
  return f"""<CollectBeads name="{step_name}" enabled="true">
                <Count>{count}</Count>
                <CollectTime>{collect_time}</CollectTime>
                <Plates><Plate id="{plate_id}" wellGroup="Plate"/></Plates>
              </CollectBeads>"""


def _release_beads_step_xml(
  step_name: str,
  plate_id: str,
  duration_sec: float,
  speed: str,
) -> str:
  """One ReleaseBeads step XML (standalone)."""
  _check_speed(speed)
  speed_guid = SPEED_GUIDS[speed]
  duration = _duration_sec_to_xml(duration_sec)
  return f"""<ReleaseBeads name="{step_name}" enabled="true">
                <Duration>{duration}</Duration>
                <Speed>{speed_guid}</Speed>
                <Plates><Plate id="{plate_id}" wellGroup="Plate"/></Plates>
              </ReleaseBeads>"""


def _pause_step_xml(step_name: str, message: str) -> str:
  """One Pause step XML (no plate ref in step; Tip still has plates)."""
  return f"""<Pause name="{step_name}" enabled="true">
                <Message>{message}</Message>
              </Pause>"""


def _assemble_bdz(protocol_name: str, properties_xml: str, exported_data_xml: str) -> bytes:
  """Compress XML blocks and prepend header."""
  header = _build_header()
  gzip1 = gzip.compress(properties_xml.encode("utf-8"))
  gzip2 = gzip.compress(exported_data_xml.encode("utf-8"))
  return header + gzip1 + gzip2


def build_mix_bdz(
  protocol_name: str,
  plate_name: str,
  duration_sec: float,
  speed: str = "Medium",
  *,
  image: str = "Wash",
  loop_count: int = 3,
  tip_plate_name: str = "DWP",
) -> bytes:
  """Build a minimal .bdz containing a single Mix step.

  Supported speeds: Medium, Fast. Unsupported (do not guess): Slow, Bottom mix, Half mix.
  """
  tip_plate_id = _guid(f"plate:{protocol_name}:{tip_plate_name}")
  step_plate_id = _guid(f"plate:{protocol_name}:{plate_name}")
  step_name = "Step1"
  step_xml = _mix_step_xml(
    step_name, step_plate_id, duration_sec, speed, image, loop_count
  )
  exported = _build_exported_data_xml(
    protocol_name, tip_plate_name, tip_plate_id, plate_name, step_plate_id,
    step_xml, step_name,
  )
  properties = _build_properties_xml(protocol_name)
  return _assemble_bdz(protocol_name, properties, exported)


def build_dry_bdz(
  protocol_name: str,
  duration_sec: float,
  *,
  plate_name: str = "Plate1",
  tip_position: str = "AboveSurface",
  tip_plate_name: str = "DWP",
) -> bytes:
  """Build a minimal .bdz containing a single Dry step."""
  tip_plate_id = _guid(f"plate:{protocol_name}:{tip_plate_name}")
  step_plate_id = _guid(f"plate:{protocol_name}:{plate_name}")
  step_name = "Dry1"
  step_xml = _dry_step_xml(step_name, step_plate_id, duration_sec, tip_position)
  exported = _build_exported_data_xml(
    protocol_name, tip_plate_name, tip_plate_id, plate_name, step_plate_id,
    step_xml, step_name,
  )
  properties = _build_properties_xml(protocol_name)
  return _assemble_bdz(protocol_name, properties, exported)


def build_collect_beads_bdz(
  protocol_name: str,
  count: int = 3,
  collect_time_sec: float = 30,
  *,
  plate_name: str = "Plate1",
  tip_plate_name: str = "DWP",
) -> bytes:
  """Build a minimal .bdz containing a single CollectBeads step. Count must be 1..5."""
  tip_plate_id = _guid(f"plate:{protocol_name}:{tip_plate_name}")
  step_plate_id = _guid(f"plate:{protocol_name}:{plate_name}")
  step_name = "CollectBeads1"
  step_xml = _collect_beads_step_xml(
    step_name, step_plate_id, count, collect_time_sec
  )
  exported = _build_exported_data_xml(
    protocol_name, tip_plate_name, tip_plate_id, plate_name, step_plate_id,
    step_xml, step_name,
  )
  properties = _build_properties_xml(protocol_name)
  return _assemble_bdz(protocol_name, properties, exported)


def build_release_beads_bdz(
  protocol_name: str,
  duration_sec: float,
  speed: str = "Fast",
  *,
  plate_name: str = "Plate1",
  tip_plate_name: str = "DWP",
) -> bytes:
  """Build a minimal .bdz containing a single ReleaseBeads step."""
  tip_plate_id = _guid(f"plate:{protocol_name}:{tip_plate_name}")
  step_plate_id = _guid(f"plate:{protocol_name}:{plate_name}")
  step_name = "ReleaseBeads1"
  step_xml = _release_beads_step_xml(
    step_name, step_plate_id, duration_sec, speed
  )
  exported = _build_exported_data_xml(
    protocol_name, tip_plate_name, tip_plate_id, plate_name, step_plate_id,
    step_xml, step_name,
  )
  properties = _build_properties_xml(protocol_name)
  return _assemble_bdz(protocol_name, properties, exported)


def build_pause_bdz(
  protocol_name: str,
  message: str = "",
  *,
  tip_plate_name: str = "DWP",
) -> bytes:
  """Build a minimal .bdz containing a single Pause step."""
  tip_plate_id = _guid(f"plate:{protocol_name}:{tip_plate_name}")
  step_plate_id = _guid(f"plate:{protocol_name}:Placeholder")
  step_name = "Pause1"
  step_xml = _pause_step_xml(step_name, message)
  exported = _build_exported_data_xml(
    protocol_name, tip_plate_name, tip_plate_id, "Placeholder", step_plate_id,
    step_xml, step_name,
  )
  properties = _build_properties_xml(protocol_name)
  return _assemble_bdz(protocol_name, properties, exported)
