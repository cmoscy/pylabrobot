"""Build minimal BindIt .bdz protocol files for single-step KingFisher Presto protocols.

Used by the liquid-handler-like API (mix, dry, collect_beads, release_beads, pause).
Only supported speed names are exposed; unsupported speeds raise ValueError.

BDZ file format (observed in 260202_test-protocol-24/96.bdz)
-------------------------------------------------------------
  [HEADER 61 bytes] [BLOCK1 gzip] [SPACER 8 bytes] [BLOCK2 gzip]

  HEADER (61 bytes): magic(4), version_flags(8), length(4), str_len(2), "BindIt Software"(15),
    version_str_len(2), version_str(8), reserved(18). Reserved encodes payload_size_minus_2
    and block1_size (see BdzHeader).

  BLOCK1: gzip stream → decompresses to Properties XML (metadata: name, creator, instrument, etc.).

  SPACER (8 bytes): prefix(4) = 01 00 00 01 constant; suffix(4) LE = block2_len - 65.
    So 65 = 61 (header len) + 4 (header "length" field size). See BdzSpacer.

  BLOCK2: gzip stream + 8 trailing bytes (01 00 00 00 00 00 00 00) → decompress the gzip part to get ExportedData XML.

  Read:  read_bdz(bdz) → (header, spacer, properties_xml, exported_data_xml)
  Write: write_bdz(header, spacer, properties_xml, exported_data_xml) → bdz  (always four args; no fallbacks)
"""

import struct
import uuid
import xml.etree.ElementTree as ET
import zlib

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

# BDZ header: 61 bytes. Layout observed in example .bdz files (260202_test-protocol-24/96.bdz).
#
# Offset  Size  Field               Meaning
# ------  ----  -----               -------
#   0     4     magic               File type (b6751cf2)
#   4     8     version_flags       Constant 01000a0001000000 in samples
#  12     4     length              Length of "product string" block that follows (2+15+2+8 = 27)
#  16     2     str_len             Length of bindit_software (15)
#  18    15     bindit_software     "BindIt Software"
#  33     2     version_str_len     Length of version_str (8)
#  35     8     version_str         e.g. "4.0.0.45"
#  43    18     reserved            See RESERVED_* below
#
# Reserved (18 bytes) sublayout (observed in 260202_test-protocol-24/96.bdz):
#   0:6   constant 0b0001000000
#   6:10  payload_size_minus_2     LE uint32 = (block1_len + 8 + block2_len) - 2
#  10:14  constant 00000001
#  14:16  block1_size               LE uint16 = length of first gzip block
#  16:18  00 00
#
BDZ_MAGIC = bytes.fromhex("b6751cf2")
_HEADER_FMT = "<4s8sIH15sH8s18s"  # magic, version_flags, length, str_len, bindit, ver_len, ver_str, reserved
assert struct.calcsize(_HEADER_FMT) == 61

RESERVED_PREFIX = bytes.fromhex("0b0001000000")  # 6 bytes constant
RESERVED_MID = bytes.fromhex("00000001")  # bytes 10:14 in samples
RESERVED_TAIL = bytes.fromhex("0000")  # bytes 16:18


def pack_reserved(payload_size_minus_2: int, block1_size: int) -> bytes:
  """Build the 18-byte reserved block from payload/block sizes (for encoding when writing)."""
  return (
    RESERVED_PREFIX
    + struct.pack("<I", payload_size_minus_2 & 0xFFFFFFFF)
    + RESERVED_MID
    + struct.pack("<H", block1_size & 0xFFFF)
    + RESERVED_TAIL
  )


class BdzHeader:
  """61-byte BDZ file header. Parse from bytes for round-trip; use default() when building from scratch."""

  __slots__ = ("magic", "version_flags", "length", "str_len", "bindit_software", "version_str_len", "version_str", "reserved")

  def __init__(
    self,
    *,
    magic: bytes = BDZ_MAGIC,
    version_flags: bytes = bytes.fromhex("01000a0001000000"),
    length: int = 27,
    str_len: int = 15,
    bindit_software: bytes = b"BindIt Software",
    version_str_len: int = 8,
    version_str: bytes = b"4.0.0.45",
    reserved: bytes | None = None,
  ):
    if reserved is None:
      reserved = pack_reserved(0, 0)  # default when building new
    assert len(magic) == 4 and len(version_flags) == 8 and len(bindit_software) == 15
    assert len(version_str) == 8 and len(reserved) == 18
    self.magic = magic
    self.version_flags = version_flags
    self.length = length
    self.str_len = str_len
    self.bindit_software = bindit_software
    self.version_str_len = version_str_len
    self.version_str = version_str
    self.reserved = reserved

  @property
  def reserved_payload_size_minus_2(self) -> int:
    """Payload size minus 2 (block1 + 8 + block2 - 2). From reserved[6:10] LE."""
    return int.from_bytes(self.reserved[6:10], "little")

  @property
  def reserved_block1_size(self) -> int:
    """First gzip block length. From reserved[14:16] LE uint16."""
    return int.from_bytes(self.reserved[14:16], "little")

  @classmethod
  def from_bytes(cls, data: bytes) -> "BdzHeader":
    if len(data) < 61:
      raise ValueError("BDZ header must be at least 61 bytes")
    parts = struct.unpack(_HEADER_FMT, data[:61])
    return cls(
      magic=parts[0],
      version_flags=parts[1],
      length=parts[2],
      str_len=parts[3],
      bindit_software=parts[4],
      version_str_len=parts[5],
      version_str=parts[6],
      reserved=parts[7],
    )

  def to_bytes(self) -> bytes:
    return struct.pack(
      _HEADER_FMT,
      self.magic,
      self.version_flags,
      self.length,
      self.str_len,
      self.bindit_software,
      self.version_str_len,
      self.version_str,
      self.reserved,
    )

  def with_reserved_from_payload(self, block1_size: int, block2_size: int) -> "BdzHeader":
    """Return a new header with reserved block set from payload sizes (for round-trip / correct encoding)."""
    payload_size = block1_size + 8 + block2_size
    return BdzHeader(
      magic=self.magic,
      version_flags=self.version_flags,
      length=self.length,
      str_len=self.str_len,
      bindit_software=self.bindit_software,
      version_str_len=self.version_str_len,
      version_str=self.version_str,
      reserved=pack_reserved(payload_size - 2, block1_size),
    )

  @classmethod
  def default(cls) -> "BdzHeader":
    """Header used when building a new .bdz (our fixed values). reserved uses 0,0 until payload is known."""
    return cls(reserved=pack_reserved(0, 0))

# Payload: header then block1 then 8-byte spacer then block2. One format, one method.
# Spacer: 8 bytes. First 4 bytes constant 01 00 00 01 (LE 0x01000001); next 4 bytes file-dependent (LE uint32).
# Empirical (260202_test-protocol-24.bdz, -96.bdz): suffix_le = block2_len - SPACER_SUFFIX_OFFSET.
# 65 = BDZ header length (61) + 4 (size of the "length" uint32 in the BDZ header at offset 12). So the suffix
# is block2 length minus that fixed overhead; not gzip header/footer (18) or "rest of file".
SPACER_SUFFIX_OFFSET = 61 + 4  # 65
_SPACER_PREFIX = bytes.fromhex("01000001")
_GZIP_MAGIC = b"\x1f\x8b"
BDZ_HEADER_LEN = 61
BDZ_SPACER_LEN = 8


class BdzSpacer:
  """8-byte spacer between block1 and block2. Parse for round-trip; use default() when building new."""

  __slots__ = ("prefix", "suffix_le")

  # First 4 bytes constant in observed files.
  PREFIX = bytes.fromhex("01000001")

  def __init__(self, *, prefix: bytes = PREFIX, suffix_le: int = 0):
    assert len(prefix) == 4, "prefix must be 4 bytes"
    self.prefix = prefix
    self.suffix_le = suffix_le & 0xFFFFFFFF

  @classmethod
  def from_bytes(cls, data: bytes) -> "BdzSpacer":
    """Parse 8-byte spacer. data may be the full 8-byte spacer or full bdz (spacer located by prefix)."""
    if len(data) == BDZ_SPACER_LEN:
      return cls(prefix=data[:4], suffix_le=int.from_bytes(data[4:8], "little"))
    if len(data) >= BDZ_HEADER_LEN:
      payload = data[BDZ_HEADER_LEN:]
      i = payload.find(_SPACER_PREFIX)
      if i >= 0 and len(payload) >= i + BDZ_SPACER_LEN:
        return cls.from_bytes(payload[i : i + BDZ_SPACER_LEN])
    raise ValueError("BDZ spacer: need 8 bytes or full bdz to locate spacer")

  def to_bytes(self) -> bytes:
    return self.prefix + struct.pack("<I", self.suffix_le)

  @classmethod
  def default(cls) -> "BdzSpacer":
    """Spacer used when building a new .bdz (suffix 0)."""
    return cls(suffix_le=0)

  @classmethod
  def from_block2_len(cls, block2_len: int) -> "BdzSpacer":
    """Spacer suffix from second gzip block length. Empirical: suffix_le = block2_len - SPACER_SUFFIX_OFFSET (65)."""
    suffix_le = max(0, block2_len - SPACER_SUFFIX_OFFSET) & 0xFFFFFFFF
    return cls(suffix_le=suffix_le)


def _spacer_index_in_payload(payload: bytes) -> int:
  """Return index of spacer start in payload (after header). Raises if not found."""
  i = payload.find(_SPACER_PREFIX)
  if i < 0 or len(payload) < i + 10 or payload[i + 8 : i + 10] != _GZIP_MAGIC:
    raise ValueError("invalid BDZ: 8-byte spacer (01 00 00 01 ... 1f 8b) not found")
  return i


# Block format (observed in example .bdz: 260202_test-protocol-24.bdz, -96.bdz):
# Each block is a complete gzip stream: 10-byte header (1f 8b 08 00 00 00 00 00 04 00), deflate body, 8-byte footer (CRC32 + ISIZE).
# Block2 in the example files has 8 trailing bytes after the gzip stream; we append them when writing for layout compatibility.
BLOCK2_TRAILING = bytes.fromhex("0100000000000000")

# Deflate compression level (1–9). We use 9 for smallest output. BindIt may use a different level or library
# (e.g. .NET GZipStream defaults to "Optimal", often equivalent to level 6), which can make their stream
# larger. Byte-identical match is unlikely without the same compressor; set to 6 to approximate .NET/Windows
# output size if round-trip size matters.
GZIP_COMPRESSION_LEVEL = 9


def _gzip_compress(data: bytes, level: int | None = None) -> bytes:
  """Produce a gzip stream (header + deflate + footer). Header matches example files (xfl=4, os=0)."""
  if level is None:
    level = GZIP_COMPRESSION_LEVEL
  level = max(1, min(9, level))
  # 10-byte gzip header as in example: magic(2), method(1)=8, flags(1)=0, mtime(4)=0, xfl(1)=4, os(1)=0
  header = bytes.fromhex("1f8b0800000000000400")
  comp = zlib.compressobj(level, zlib.DEFLATED, -15)  # raw deflate, no zlib wrapper
  body = comp.compress(data) + comp.flush()
  crc = (zlib.crc32(data) & 0xFFFFFFFF).to_bytes(4, "little")
  isize = (len(data) & 0xFFFFFFFF).to_bytes(4, "little")
  return header + body + crc + isize

# Deterministic GUID namespace for plates/steps (same inputs -> same .bdz for tests).
_NAMESPACE_BDZ = uuid.UUID("7c9e6679-7425-40de-944b-e07fc1f90ae7")


def split_bdz_payload(bdz: bytes) -> tuple[bytes, bytes]:
  """Return (gzip_block1, gzip_block2) from .bdz bytes.

  Format: 61-byte header, gzip1, 8-byte spacer (01 00 00 01 + 4 bytes), gzip2.
  Find spacer by the 4-byte prefix then require 1f 8b at +8; no fallback.
  """
  if len(bdz) < BDZ_HEADER_LEN + 2 or bdz[:4] != BDZ_MAGIC:
    raise ValueError("invalid BDZ: bad magic or too short")
  payload = bdz[BDZ_HEADER_LEN:]
  if payload[:2] != _GZIP_MAGIC:
    raise ValueError("invalid BDZ: payload does not start with gzip")
  i = _spacer_index_in_payload(payload)
  return payload[:i], payload[i + BDZ_SPACER_LEN:]


def get_bdz_spacer(bdz: bytes) -> BdzSpacer:
  """Return the 8-byte spacer from a .bdz file (for round-trip)."""
  if len(bdz) < BDZ_HEADER_LEN + BDZ_SPACER_LEN or bdz[:4] != BDZ_MAGIC:
    raise ValueError("invalid BDZ: bad magic or too short")
  payload = bdz[BDZ_HEADER_LEN:]
  i = _spacer_index_in_payload(payload)
  return BdzSpacer.from_bytes(payload[i : i + BDZ_SPACER_LEN])


def decompress_bdz(bdz: bytes) -> tuple[bytes, bytes]:
  """Return (properties_xml, exported_data_xml) as bytes. Uses split_bdz_payload."""
  gzip1, gzip2 = split_bdz_payload(bdz)
  # zlib with wbits=16+15 handles gzip and works for both our builder and BindIt-authored files.
  return zlib.decompress(gzip1, 16 + 15), zlib.decompress(gzip2, 16 + 15)


def read_bdz(bdz: bytes) -> tuple[BdzHeader, BdzSpacer, bytes, bytes]:
  """Read a .bdz into header, spacer, and the two XML payloads.

  Returns (header, spacer, properties_xml, exported_data_xml). Use for round-trip:
  pass header and spacer into write_bdz to preserve non-compressed parts exactly.
  """
  header = BdzHeader.from_bytes(bdz)
  spacer = get_bdz_spacer(bdz)
  properties_xml, exported_data_xml = decompress_bdz(bdz)
  return (header, spacer, properties_xml, exported_data_xml)


def write_bdz(
  header: BdzHeader,
  spacer: BdzSpacer,
  properties_xml: str | bytes,
  exported_data_xml: str | bytes,
) -> bytes:
  """Assemble a .bdz: header + gzip(properties_xml) + spacer + gzip(exported_data_xml).

  Format is always header, block1, spacer, block2. Caller must provide header and spacer.
  For round-trip: pass header and spacer from read_bdz() (reserved is updated from actual block sizes).
  For new files: compress first, then header = BdzHeader.default().with_reserved_from_payload(len(block1), len(block2)), spacer = BdzSpacer.from_block2_len(len(block2)), then write_bdz(header, spacer, p1, p2).
  """
  p1 = properties_xml.encode("utf-8") if isinstance(properties_xml, str) else properties_xml
  p2 = exported_data_xml.encode("utf-8") if isinstance(exported_data_xml, str) else exported_data_xml
  block1 = _gzip_compress(p1)
  block2 = _gzip_compress(p2) + BLOCK2_TRAILING
  h = header.with_reserved_from_payload(len(block1), len(block2))
  return h.to_bytes() + block1 + spacer.to_bytes() + block2


def _step_tag_local(e: ET.Element) -> str:
  """Local name of element tag (strip namespace)."""
  return e.tag.split("}")[-1] if "}" in e.tag else e.tag


def set_tip_enabled_in_exported_data(exported_data_xml: bytes, enabled: bool) -> bytes:
  """Set the Tip element's enabled attribute in ExportedData XML. Returns new XML bytes."""
  root = ET.fromstring(exported_data_xml)
  for el in root.iter():
    if _step_tag_local(el) == "Tip":
      el.set("enabled", "true" if enabled else "false")
      break
  return ET.tostring(root, encoding="utf-8", method="xml", xml_declaration=True)


def parse_exported_data_steps(exported_data_xml: bytes) -> list[dict]:
  """Parse ExportedData XML into a unified step list for inspection.

  Returns a list of dicts: [{"type": "Mix", "name": "Mix1", ...}, ...].
  Keys depend on step type (duration, speed, count, message, etc.).
  Use this to examine protocol structure and compare across .bdz files.
  """
  root = ET.fromstring(exported_data_xml)
  steps: list[dict] = []
  for tip in root.iter():
    if _step_tag_local(tip) != "Tip":
      continue
    for steps_el in tip:
      if _step_tag_local(steps_el) != "Steps":
        continue
      for step_el in steps_el:
        tag = _step_tag_local(step_el)
        d: dict = {"type": tag, "name": step_el.get("name", "")}
        for child in step_el:
          ctag = _step_tag_local(child)
          if child.text and child.text.strip():
            d[ctag] = child.text.strip()
          elif len(child) == 0 and child.text is None and list(child) == []:
            d[ctag] = None
        steps.append(d)
      break
    break
  return steps


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
  """Build a .bdz: header + block1 + spacer + block2. Always the same structure."""
  block1 = _gzip_compress(properties_xml.encode("utf-8"))
  block2 = _gzip_compress(exported_data_xml.encode("utf-8")) + BLOCK2_TRAILING
  header = BdzHeader.default().with_reserved_from_payload(len(block1), len(block2))
  spacer = BdzSpacer.from_block2_len(len(block2))
  return write_bdz(header, spacer, properties_xml, exported_data_xml)


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
