"""Internal representation of KingFisher Presto protocols and BDZ files.

Protocols are modeled as KingFisherProtocol with plates and tips; each Tip has
a tip comb plate and an ordered list of steps. Build programmatically and
emit compatible .bdz via to_bdz(); parse existing .bdz via parse_bdz_to_protocol().
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from .bdz_builder import (
    BdzHeader,
    INSTRUMENT_TYPE_ID,
    POSTMIX_SPEED_GUID,
    SPEED_GUIDS,
    read_bdz,
    write_bdz,
)
from .bdz_builder import _check_speed as check_speed
from .bdz_builder import _duration_sec_to_xml as duration_sec_to_xml
from .bdz_builder import _guid as guid


# Null plate ID for steps with no plate (Dry, Pause, some Mix).
NULL_PLATE_ID = "00000000-0000-0000-0000-000000000000"


# --- PlateType ---

class PlateType(Enum):
  """Known plate types with GUID and dimensions from example BDZ."""
  DWP_96 = ("8b7d7c98-275f-4285-8129-5f8ed46fb01e", 12, 8)
  DWP_24 = ("0d419a5c-1dc6-425f-8d9d-0b18481869cc", 6, 4)
  WP_96 = ("5d3a0092-8ae3-4d66-a17c-0ba1d569f115", 12, 8)
  TIPS_96 = ("8b7d7c98-275f-4285-8129-5f8ed46fb01e", 12, 8)
  TIPS_24 = ("0d419a5c-1dc6-425f-8d9d-0b18481869cc", 6, 4)

  @property
  def plate_type_id(self) -> str:
    return self.value[0]

  @property
  def columns(self) -> int:
    return self.value[1]

  @property
  def rows(self) -> int:
    return self.value[2]

  @property
  def region_x(self) -> int:
    return 0

  @property
  def region_y(self) -> int:
    return 0


# --- Plate ---

@dataclass
class Plate:
  """A plate in the protocol (PlateLayout). Type and dimensions from PlateType."""
  name: str
  plate_type: PlateType
  volume: float | None = None
  group_id: str | None = None
  reagent_name: str | None = None
  color: str | None = None
  reagent_type: str | None = None
  _id: str | None = None  # Set at write time from protocol name + name

  @classmethod
  def create(
    cls,
    name: str,
    plate_type: PlateType,
    volume: float | None = None,
    **kwargs: Any,
  ) -> Plate:
    """Create a Plate with explicit type. Extra kwargs stored for optional Container/Reagent."""
    return cls(name=name, plate_type=plate_type, volume=volume, **kwargs)


# --- TipPosition (shared by Dry and Mix) ---

class TipPosition(Enum):
  OutsideWellTube = "OutsideWellTube"
  AboveSurface = "AboveSurface"
  EdgeInLiquid = "EdgeInLiquid"


# --- Image (Mix step icon) ---

class Image(Enum):
  Mix = "Mix"
  Heating = "Heating"
  Bind = "Bind"
  Wash = "Wash"
  Elution = "Elution"


# --- Step base and concrete steps ---
# Schema-driven parse/write: (field_or_tag, path_or_tag, default_or_serializer, parse_fn or None).
# Parse schema: (field_name, path, default_str, parse_fn). Write schema: (tag, get_value, serialize_fn).

def _parse_int(el: ET.Element, path: str, default: int) -> int:
  raw = el.findtext(path, str(default))
  return int(raw.strip()) if raw and raw.strip() else default


def _parse_duration_sec(el: ET.Element, path: str, default_sec: float) -> float:
  raw = el.findtext(path)
  return _parse_duration_to_sec(raw.strip()) if raw and raw.strip() else default_sec


def _parse_enum(el: ET.Element, path: str, enum_cls: type[Enum], default: Enum) -> Enum:
  raw = el.findtext(path)
  if not raw or not raw.strip():
    return default
  try:
    return enum_cls(raw.strip())
  except ValueError:
    return default


def _parse_attr_bool(el: ET.Element, path: str, attr: str = "enabled", default: bool = False) -> bool:
  child = el.find(path)
  if child is None:
    return default
  return (child.get(attr) or "").strip().lower() == "true"


def _elements_from_schema(
  step: Any,
  schema: list[tuple[str, Any, Any]],
) -> list[ET.Element]:
  """Build list of ET elements from (tag, get_value, serialize_fn) schema."""
  out: list[ET.Element] = []
  for tag, get_val, serialize in schema:
    el = ET.Element(tag)
    el.text = str(serialize(get_val(step)))
    out.append(el)
  return out


# CollectBeads: CollectCount, CollectTime (standalone uses CollectCount not Count).
_COLLECT_BEADS_WRITE_SCHEMA = [
  ("CollectCount", lambda s: s.count, str),
  ("CollectTime", lambda s: s.collect_time_sec, duration_sec_to_xml),
]

# ReleaseBeads: Duration, Speed (speed -> GUID in writer).
# Dry: Duration, TipPosition.
_DRY_WRITE_SCHEMA = [
  ("Duration", lambda s: s.duration_sec, duration_sec_to_xml),
  ("TipPosition", lambda s: s.tip_position.value, str),
]


@dataclass
class CollectBeadsStep:
  name: str
  enabled: bool = True
  plate_ref: tuple[str, str] | None = None
  count: int = 3
  collect_time_sec: float = 5.0

  def params_to_xml_elements(self) -> list[ET.Element]:
    return _elements_from_schema(self, _COLLECT_BEADS_WRITE_SCHEMA)


@dataclass
class ReleaseBeadsStep:
  name: str
  enabled: bool = True
  plate_ref: tuple[str, str] | None = None
  duration_sec: float = 5.0
  speed: str = "Fast"

  def params_to_xml_elements(self) -> list[ET.Element]:
    speed_guid = _speed_to_guid(self.speed)
    return [
      _el_text("Duration", duration_sec_to_xml(self.duration_sec)),
      _el_text("Speed", speed_guid),
    ]


@dataclass
class DryStep:
  name: str
  enabled: bool = True
  plate_ref: tuple[str, str] | None = None
  duration_sec: float = 300.0
  tip_position: TipPosition = TipPosition.AboveSurface

  def params_to_xml_elements(self) -> list[ET.Element]:
    return _elements_from_schema(self, _DRY_WRITE_SCHEMA)


def _el_text(tag: str, text: str) -> ET.Element:
  el = ET.Element(tag)
  el.text = text
  return el


@dataclass
class PauseStep:
  name: str
  enabled: bool = True
  plate_ref: tuple[str, str] | None = None
  message: str = ""
  dispense_enabled: bool = False
  volume_ul: float | None = None
  reagent_name: str | None = None

  def params_to_xml_elements(self) -> list[ET.Element]:
    el = ET.Element("Message")
    el.text = self.message or ""
    disp = ET.Element("Dispense", enabled="true" if self.dispense_enabled else "false")
    disp.append(ET.Element("Reagents"))
    return [el, disp]


# Mix sub-step data for MixStep
@dataclass
class MixShake:
  duration_sec: float
  speed: str  # name or GUID; writer uses SPEED_GUIDS when name


@dataclass
class MixStep:
  name: str
  enabled: bool = True
  plate_ref: tuple[str, str] | None = None
  image: Image = Image.Mix
  precollect_enabled: bool = False
  # ReleaseBeads
  release_beads_enabled: bool = False
  release_beads_duration_sec: float = 0.0
  release_beads_speed: str = "Fast"
  # Mixing
  shakes: list[MixShake] = field(default_factory=list)
  loop_count: int = 3
  pause_tip_position: TipPosition = TipPosition.AboveSurface
  # Pause
  pause_enabled: bool = False
  pause_message: str = ""
  # Heating
  heating_temperature: float = 0.0
  heating_preheat: bool = False
  # Postmix
  postmix_enabled: bool = False
  postmix_duration_sec: float = 0.0
  postmix_speed: str = "Medium"
  # CollectBeads
  collect_beads_enabled: bool = True
  collect_beads_count: int = 3
  collect_beads_time_sec: float = 30.0
  # PostTemperature
  post_temperature_enabled: bool = False
  post_temperature: float = 0.0

  def params_to_xml_elements(self) -> list[ET.Element]:
    out: list[ET.Element] = []
    # Image
    el = ET.Element("Image")
    el.text = self.image.value
    out.append(el)
    # Precollect
    out.append(_precollect_el(self.precollect_enabled))
    # ReleaseBeads
    out.append(_release_beads_el(
      self.release_beads_enabled,
      self.release_beads_duration_sec,
      self.release_beads_speed,
    ))
    # Mixing
    out.append(_mixing_el(self.shakes, self.loop_count, self.pause_tip_position))
    # Pause
    out.append(_pause_el(self.pause_enabled, self.pause_message))
    # Heating
    out.append(_heating_el(self.heating_temperature, self.heating_preheat))
    # Postmix
    out.append(_postmix_el(self.postmix_enabled, self.postmix_duration_sec, self.postmix_speed))
    # CollectBeads
    out.append(_collect_beads_inline_el(
      self.collect_beads_enabled,
      self.collect_beads_count,
      self.collect_beads_time_sec,
    ))
    # PostTemperature
    out.append(_post_temperature_el(self.post_temperature_enabled, self.post_temperature))
    return out


# Mix sub-builders (ET elements only)
def _precollect_el(enabled: bool) -> ET.Element:
  return ET.Element("Precollect", enabled="true" if enabled else "false")


def _release_beads_el(enabled: bool, duration_sec: float, speed: str) -> ET.Element:
  speed_guid = _speed_to_guid(speed)
  el = ET.Element("ReleaseBeads", enabled="true" if enabled else "false")
  d = ET.SubElement(el, "Duration")
  d.text = duration_sec_to_xml(duration_sec)
  s = ET.SubElement(el, "Speed")
  s.text = speed_guid
  return el


def _speed_to_guid(speed: str) -> str:
  """Return GUID for speed (name or already-GUID)."""
  if "-" in speed and len(speed) >= 32:
    return speed  # already a GUID from parsed BDZ
  check_speed(speed)
  return SPEED_GUIDS[speed]


def _mixing_el(
  shakes: list[MixShake],
  loop_count: int,
  pause_tip_position: TipPosition,
) -> ET.Element:
  el = ET.Element("Mixing")
  shakes_el = ET.SubElement(el, "Shakes")
  for sh in shakes[:3]:
    speed_guid = _speed_to_guid(sh.speed)
    ET.SubElement(
      shakes_el, "Shake",
      duration=duration_sec_to_xml(sh.duration_sec),
      speed=speed_guid,
    )
  lc = ET.SubElement(el, "LoopCount")
  lc.text = str(loop_count)
  pt = ET.SubElement(el, "PauseTipPosition")
  pt.text = pause_tip_position.value
  return el


def _pause_el(enabled: bool, message: str) -> ET.Element:
  el = ET.Element("Pause", enabled="true" if enabled else "false")
  m = ET.SubElement(el, "Message")
  m.text = message or ""
  return el


def _heating_el(temperature: float, preheat: bool) -> ET.Element:
  el = ET.Element("Heating", enabled="true")
  t = ET.SubElement(el, "Temperature")
  t.text = str(int(temperature))
  p = ET.SubElement(el, "Preheat")
  p.text = "true" if preheat else "false"
  return el


def _postmix_el(enabled: bool, duration_sec: float, speed: str) -> ET.Element:
  el = ET.Element("Postmix", enabled="true" if enabled else "false")
  d = ET.SubElement(el, "Duration")
  d.text = duration_sec_to_xml(duration_sec)
  s = ET.SubElement(el, "Speed")
  s.text = POSTMIX_SPEED_GUID if speed == "Medium" else SPEED_GUIDS.get(speed, speed)
  return el


def _collect_beads_inline_el(enabled: bool, count: int, collect_time_sec: float) -> ET.Element:
  el = ET.Element("CollectBeads", enabled="true" if enabled else "false")
  c = ET.SubElement(el, "Count")
  c.text = str(count)
  t = ET.SubElement(el, "CollectTime")
  t.text = duration_sec_to_xml(collect_time_sec)
  return el


def _post_temperature_el(enabled: bool, temperature: float) -> ET.Element:
  el = ET.Element("PostTemperature", enabled="true" if enabled else "false")
  t = ET.SubElement(el, "Temperature")
  t.text = str(int(temperature))
  return el


# --- Tip ---

StepT = CollectBeadsStep | ReleaseBeadsStep | DryStep | PauseStep | MixStep


@dataclass
class Tip:
  """One tip comb: name, plate (tip comb plate), and ordered steps."""
  name: str
  plate: Plate | str
  steps: list[StepT] = field(default_factory=list)


# --- resolve_plate_ref and step_to_xml_element ---

def resolve_plate_ref(
  step: StepT,
  plate_id_by_name: dict[str, str],
) -> tuple[str, str]:
  """Return (plate_id, well_group) for the step. Null plate -> (NULL_PLATE_ID, '')."""
  if step.plate_ref is None:
    return (NULL_PLATE_ID, "")
  plate_id, well_group = step.plate_ref
  if plate_id == NULL_PLATE_ID or plate_id == "":
    return (NULL_PLATE_ID, "")
  return (plate_id, well_group or "Plate")


def step_to_xml_element(
  step: StepT,
  plate_id: str,
  well_group: str,
) -> ET.Element:
  """Build one step XML element (root + params + Plates/LegacyParameters/Steps)."""
  if isinstance(step, CollectBeadsStep):
    tag = "CollectBeads"
  elif isinstance(step, ReleaseBeadsStep):
    tag = "ReleaseBeads"
  elif isinstance(step, DryStep):
    tag = "Dry"
  elif isinstance(step, PauseStep):
    tag = "Pause"
  elif isinstance(step, MixStep):
    tag = "Mix"
  else:
    raise TypeError(type(step))
  root = ET.Element(tag, name=step.name, enabled="true" if step.enabled else "false")
  for child in step.params_to_xml_elements():
    root.append(child)
  plates = ET.SubElement(root, "Plates")
  ET.SubElement(plates, "Plate", id=plate_id, wellGroup=well_group)
  root.append(ET.Element("LegacyParameters"))
  root.append(ET.Element("Steps"))
  return root


# --- PlateLayout and ExportedData builders ---

def _build_plate_layout_xml(
  protocol_name: str,
  plates: list[Plate],
  plate_id_by_name: dict[str, str],
  group_id_by_name: dict[str, str] | None = None,
) -> ET.Element:
  """Build PlateLayout element: ID, Name, Plates with Wells/Group/Region."""
  if group_id_by_name is None:
    group_id_by_name = {}
  layout_id = guid(f"plate_layout:{protocol_name}")
  plate_layout = ET.Element("PlateLayout", ID=layout_id, Name="No name")
  plate_layout.append(ET.Element("Description"))
  plate_layout.append(ET.Element("PlateTemplates"))
  plates_el = ET.SubElement(plate_layout, "Plates")
  for plate in plates:
    pid = guid(f"plate:{protocol_name}:{plate.name}")
    plate_id_by_name[plate.name] = pid
    plate._id = pid
    pe = ET.SubElement(
      plates_el, "Plate",
      id=pid, name=plate.name, plateTypeID=plate.plate_type.plate_type_id,
    )
    wells = ET.SubElement(pe, "Wells")
    group_id = guid(f"group:{protocol_name}:{plate.name}")
    group_id_by_name[plate.name] = group_id
    gr = ET.SubElement(wells, "Group", ID=group_id, name="Plate")
    ET.SubElement(gr, "Region", x="0", y="0", columns=str(plate.plate_type.columns), rows=str(plate.plate_type.rows))
  return plate_layout


def _build_properties_xml(protocol_name: str, run_id: str | None = None) -> str:
  """Properties XML for block1. Structure follows BDZ build spec (bdz_builder docstring): Properties > ExportedObject, Flags, InstrumentParameters.
  If run_id is provided, use it for ExportedObject @id so it matches Run @ID and Protocol @ID in ExportedData (BindIt expects these to match)."""
  obj_id = run_id if run_id else guid(f"properties:{protocol_name}")
  return (
    f'<Properties version="1">'
    f'<ExportedObject name="{protocol_name}" id="{obj_id}">'
    f"<InstrumentTypeId>{INSTRUMENT_TYPE_ID}</InstrumentTypeId>"
    f"<CreatorName>pylabrobot</CreatorName>"
    f"<Timestamp>0001-01-01T00:00:00</Timestamp>"
    f"<ExecutionTime>0001-01-01T00:00:00</ExecutionTime>"
    f"</ExportedObject>"
    f"<Flags><FactoryData>false</FactoryData></Flags>"
    f'<InstrumentParameters type="713" oemTypeId="00000000-0000-0000-0000-000000000000"><ProtocolType>1</ProtocolType></InstrumentParameters>'
    f"</Properties>"
  )


def _build_exported_data_xml(protocol: "KingFisherProtocol", run_id: str | None = None) -> bytes:
  """Build full ExportedData XML as bytes (UTF-8). If run_id is provided, use it for Run @ID and Protocol @ID (BindIt expects these to match Properties/ExportedObject @id)."""
  plate_id_by_name: dict[str, str] = {}
  group_id_by_name: dict[str, str] = {}
  run_id = run_id or guid(f"run:{protocol.name}")
  protocol_id = run_id  # BindIt expects Protocol @ID == Run @ID == Properties ExportedObject @id

  root = ET.Element("ExportedData")
  root.set("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
  root.set("xmlns:xsd", "http://www.w3.org/2001/XMLSchema")
  protocol_outer = ET.SubElement(root, "Protocol")
  run = ET.SubElement(protocol_outer, "Run", ID=run_id)
  run.append(_build_plate_layout_xml(protocol.name, protocol.plates, plate_id_by_name, group_id_by_name))

  run_spec = ET.SubElement(run, "RunSpecificInformation", Locked="false")
  si = ET.SubElement(run_spec, "SampleInformation")
  dims = ET.SubElement(si, "Dimensions")
  ET.SubElement(dims, "Width").text = "0"
  ET.SubElement(dims, "Height").text = "0"
  ET.SubElement(si, "Samples")
  ci = ET.SubElement(run_spec, "ConsumableInformation")
  ET.SubElement(ci, "Consumables")

  run_def = ET.SubElement(run, "RunDef")
  ui = ET.SubElement(run_def, "UIResultRelations")
  exec_info = ET.SubElement(ui, "ExecutedRunInformation")
  ET.SubElement(exec_info, "StartExecution")
  ET.SubElement(exec_info, "ExecutorName")
  ET.SubElement(exec_info, "Warnings")
  ET.SubElement(exec_info, "Errors")
  inst = ET.SubElement(ui, "InstrumentInformation")
  ET.SubElement(inst, "InstrumentType").text = "00000000-0000-0000-0000-000000000000"
  ET.SubElement(inst, "eInstrumentName")
  ET.SubElement(inst, "eInstrumentVersion")
  ET.SubElement(inst, "eInstrumentSerialNumber")
  ET.SubElement(ui, "SoftwareInformation").append(ET.Element("Version"))
  ET.SubElement(ui, "LaboratoryInformation")

  dx = ET.SubElement(run, "DXReports")
  sel = ET.SubElement(dx, "SelectedReports")
  rep = ET.SubElement(sel, "Report", ReportType="ProtocolReport")
  secs = ET.SubElement(rep, "SelectedSections")
  for sec in ("GeneralProtocol", "Carrier", "Dispensed", "StepsData"):
    ET.SubElement(secs, "ReportSection").text = sec

  protocol_el = ET.SubElement(
    run, "Protocol",
    name=protocol.name, ID=protocol_id, locked="false", IsExecutable="false", enabled="true",
  )  # protocol_id == run_id so BindIt sees consistent top-level ID
  ET.SubElement(protocol_el, "KitName")
  ET.SubElement(protocol_el, "RemovePlateMessage")
  ET.SubElement(protocol_el, "ProtocolType").text = "Normal"
  # Plates that get a Container: all except tip plate(s). Volume on Plate is required for correct heights on mixing/other steps.
  tip_plate_names = {
    (tip.plate.name if isinstance(tip.plate, Plate) else tip.plate) for tip in protocol.tips
  }
  plates_for_containers = [p for p in protocol.plates if p.name not in tip_plate_names]
  containers_el = ET.SubElement(protocol_el, "Containers")
  for plate in plates_for_containers:
    container_id = guid(f"container:{protocol.name}:{plate.name}")
    group_id = group_id_by_name.get(plate.name, guid(f"group:{protocol.name}:{plate.name}"))
    container_el = ET.SubElement(containers_el, "Container", id=container_id, groupId=group_id)
    contents_el = ET.SubElement(container_el, "Contents")
    reagent_id = guid(f"reagent:{protocol.name}:{plate.name}")
    reagent_name = plate.reagent_name if plate.reagent_name is not None else "None"
    reagent_volume = plate.volume if plate.volume is not None else 0
    volume_str = str(int(reagent_volume)) if reagent_volume == int(reagent_volume) else str(reagent_volume)
    reagent_color = plate.color if plate.color is not None else "ffff0000"
    reagent_type = plate.reagent_type if plate.reagent_type is not None else "Other"
    ET.SubElement(
      contents_el, "Reagent",
      id=reagent_id, name=reagent_name, volume=volume_str, color=reagent_color, type=reagent_type,
    )
  ET.SubElement(protocol_el, "LegacySpeeds")
  ET.SubElement(protocol_el, "InstrumentTypeID").text = INSTRUMENT_TYPE_ID
  ET.SubElement(protocol_el, "Description")

  steps_outer = ET.SubElement(protocol_el, "Steps")
  for tip in protocol.tips:
    tip_plate = tip.plate
    if isinstance(tip_plate, Plate):
      tip_plate_id = plate_id_by_name.get(tip_plate.name)
      if not tip_plate_id:
        tip_plate_id = guid(f"plate:{protocol.name}:{tip_plate.name}")
        plate_id_by_name[tip_plate.name] = tip_plate_id
    else:
      tip_plate_id = plate_id_by_name.get(tip_plate, guid(f"plate:{protocol.name}:{tip_plate}"))
    tip_id = guid(f"tip:{tip.name}:{protocol.name}")
    tip_persistent_id = guid(f"tip_persistent:{tip.name}:{protocol.name}")
    tip_el = ET.SubElement(
      steps_outer, "Tip",
      name=tip.name, id=tip_id, persistentID=tip_persistent_id, enabled="true",
    )
    plates_el = ET.SubElement(tip_el, "Plates")
    ET.SubElement(plates_el, "Plate", id=tip_plate_id, wellGroup="Plate")
    ET.SubElement(plates_el, "Plate", id=tip_plate_id, wellGroup="Plate")
    tip_el.append(ET.Element("LegacyParameters"))
    steps_el = ET.SubElement(tip_el, "Steps")
    for step in tip.steps:
      pid, wg = resolve_plate_ref(step, plate_id_by_name)
      steps_el.append(step_to_xml_element(step, pid, wg))

  root.append(ET.Element("RunLog"))
  return ET.tostring(root, encoding="utf-8", method="xml", xml_declaration=False)


# --- KingFisherProtocol ---

@dataclass
class KingFisherProtocol:
  """Protocol with plates and tips (steps live under each Tip)."""
  name: str
  plates: list[Plate] = field(default_factory=list)
  tips: list[Tip] = field(default_factory=list)

  def add_plate(self, plate: Plate) -> bool:
    """Add plate to the protocol. Returns True if added, False if a plate with the same name already exists (list unchanged)."""
    for p in self.plates:
      if p.name == plate.name:
        return False
    self.plates.append(plate)
    return True

  def add_tip(self, tip: Tip) -> bool:
    """Add tip to the protocol. Returns True if added, False if a tip with the same name already exists (list unchanged). The tip's plate is added to protocol.plates if needed."""
    for t in self.tips:
      if t.name == tip.name:
        if isinstance(tip.plate, Plate):
          self.add_plate(tip.plate)  # ensure plate still in protocol
        return False
    if isinstance(tip.plate, Plate):
      self.add_plate(tip.plate)
    self.tips.append(tip)
    return True

  def add_step(self, step: StepT, tip_name: str | None = None) -> None:
    """Append step to tips[0] when exactly one tip, or to the Tip with given name."""
    if tip_name is not None:
      for t in self.tips:
        if t.name == tip_name:
          t.steps.append(step)
          return
      raise ValueError(f"No tip named {tip_name!r}")
    if len(self.tips) == 1:
      self.tips[0].steps.append(step)
      return
    raise ValueError("tip_name required when protocol has more than one tip")

  @property
  def default_tip(self) -> Tip | None:
    """Return tips[0] when exactly one tip."""
    return self.tips[0] if len(self.tips) == 1 else None

  def add_collect_beads(
    self,
    name: str,
    plate_name_or_plate: str | Plate,
    count: int,
    collect_time_sec: float,
    tip_name: str | None = None,
    enabled: bool = True,
  ) -> CollectBeadsStep:
    if not 1 <= count <= 5:
      raise ValueError("Collect beads count must be 1..5")
    plate_id, plate_ref = _resolve_plate_for_step(self, plate_name_or_plate)
    step = CollectBeadsStep(
      name=name, enabled=enabled, plate_ref=plate_ref, count=count, collect_time_sec=collect_time_sec,
    )
    self.add_step(step, tip_name=tip_name)
    return step

  def add_release_beads(
    self,
    name: str,
    plate_name_or_plate: str | Plate,
    duration_sec: float,
    speed: str = "Fast",
    tip_name: str | None = None,
    enabled: bool = True,
  ) -> ReleaseBeadsStep:
    plate_id, plate_ref = _resolve_plate_for_step(self, plate_name_or_plate)
    step = ReleaseBeadsStep(
      name=name, enabled=enabled, plate_ref=plate_ref, duration_sec=duration_sec, speed=speed,
    )
    self.add_step(step, tip_name=tip_name)
    return step

  def add_dry(
    self,
    name: str,
    duration_sec: float,
    tip_position: TipPosition = TipPosition.AboveSurface,
    tip_name: str | None = None,
    enabled: bool = True,
    plate_ref: tuple[str, str] | None = None,
  ) -> DryStep:
    step = DryStep(
      name=name, enabled=enabled, plate_ref=plate_ref or (NULL_PLATE_ID, ""),
      duration_sec=duration_sec, tip_position=tip_position,
    )
    self.add_step(step, tip_name=tip_name)
    return step

  def add_pause(
    self,
    name: str,
    message: str = "",
    tip_name: str | None = None,
    enabled: bool = True,
    dispense_enabled: bool = False,
    volume_ul: float | None = None,
    reagent_name: str | None = None,
  ) -> PauseStep:
    step = PauseStep(
      name=name, enabled=enabled, plate_ref=(NULL_PLATE_ID, ""),
      message=message, dispense_enabled=dispense_enabled, volume_ul=volume_ul, reagent_name=reagent_name,
    )
    self.add_step(step, tip_name=tip_name)
    return step

  def add_mix(
    self,
    name: str,
    plate_name_or_plate: str | Plate | None,
    tip_name: str | None = None,
    enabled: bool = True,
    image: Image = Image.Mix,
    precollect_enabled: bool = False,
    release_beads_enabled: bool = False,
    release_beads_duration_sec: float = 0.0,
    release_beads_speed: str = "Fast",
    shakes: list[MixShake] | None = None,
    loop_count: int = 3,
    pause_tip_position: TipPosition = TipPosition.AboveSurface,
    pause_enabled: bool = False,
    pause_message: str = "",
    heating_temperature: float = 0.0,
    heating_preheat: bool = False,
    postmix_enabled: bool = False,
    postmix_duration_sec: float = 0.0,
    postmix_speed: str = "Medium",
    collect_beads_enabled: bool = True,
    collect_beads_count: int = 3,
    collect_beads_time_sec: float = 30.0,
    post_temperature_enabled: bool = False,
    post_temperature: float = 0.0,
    **kwargs: Any,
  ) -> MixStep:
    if plate_name_or_plate is not None:
      _, plate_ref = _resolve_plate_for_step(self, plate_name_or_plate)
    else:
      plate_ref = (NULL_PLATE_ID, "")
    step = MixStep(
      name=name, enabled=enabled, plate_ref=plate_ref,
      image=image, precollect_enabled=precollect_enabled,
      release_beads_enabled=release_beads_enabled,
      release_beads_duration_sec=release_beads_duration_sec,
      release_beads_speed=release_beads_speed,
      shakes=shakes or [], loop_count=loop_count, pause_tip_position=pause_tip_position,
      pause_enabled=pause_enabled, pause_message=pause_message,
      heating_temperature=heating_temperature, heating_preheat=heating_preheat,
      postmix_enabled=postmix_enabled, postmix_duration_sec=postmix_duration_sec, postmix_speed=postmix_speed,
      collect_beads_enabled=collect_beads_enabled, collect_beads_count=collect_beads_count,
      collect_beads_time_sec=collect_beads_time_sec,
      post_temperature_enabled=post_temperature_enabled, post_temperature=post_temperature,
    )
    self.add_step(step, tip_name=tip_name)
    return step

  def to_bdz(self) -> bytes:
    """Build BDZ bytes from this protocol (header, Properties, ExportedData built from scratch per BDZ build spec in bdz_builder).
    Uses a single run_id for Properties/ExportedObject @id, Run @ID, and Protocol @ID so BindIt can parse the file."""
    run_id = guid(f"run:{self.name}")
    properties_xml = _build_properties_xml(self.name, run_id=run_id)
    exported_data_xml = _build_exported_data_xml(self, run_id=run_id)
    return write_bdz(BdzHeader.default(), properties_xml, exported_data_xml)


def _resolve_plate_for_step(
  protocol: KingFisherProtocol,
  plate_name_or_plate: str | Plate,
) -> tuple[str | None, tuple[str, str] | None]:
  """Resolve plate to (plate_id, plate_ref). Adds plate to protocol if needed (when passed as Plate)."""
  if isinstance(plate_name_or_plate, Plate):
    protocol.add_plate(plate_name_or_plate)
    plate_id = guid(f"plate:{protocol.name}:{plate_name_or_plate.name}")
    return (plate_id, (plate_id, "Plate"))
  name = plate_name_or_plate
  for p in protocol.plates:
    if p.name == name:
      plate_id = guid(f"plate:{protocol.name}:{p.name}")
      return (plate_id, (plate_id, "Plate"))
  raise ValueError(f"Plate {name!r} not in protocol; add it first or pass a Plate instance.")


# --- Parsing ---

def _tag_local(e: ET.Element) -> str:
  return e.tag.split("}")[-1] if "}" in e.tag else e.tag


def parse_bdz_to_protocol(bdz: bytes) -> KingFisherProtocol:
  """Parse a .bdz into KingFisherProtocol (ExportedData → plates, tips, steps)."""
  header, spacer, properties_xml, exported_data_xml = read_bdz(bdz)
  root = ET.fromstring(exported_data_xml)
  protocol_el = root.find(".//Protocol[@name]")
  if protocol_el is None:
    protocol_el = root.find("Protocol/Run/Protocol")
  if protocol_el is None:
    raise ValueError("ExportedData: no Protocol element with name")
  protocol_name = protocol_el.get("name", "Protocol")

  plates: list[Plate] = []
  plate_id_to_plate: dict[str, Plate] = {}
  group_id_to_plate: dict[str, Plate] = {}
  plate_layout = root.find(".//PlateLayout")
  if plate_layout is not None:
    for pe in plate_layout.findall(".//Plates/Plate"):
      pid = pe.get("id")
      name = pe.get("name", "")
      plate_type_id = pe.get("plateTypeID", "")
      region = pe.find(".//Region")
      if region is not None:
        cols = int(region.get("columns", 12))
        rows = int(region.get("rows", 8))
      else:
        cols, rows = 12, 8
      pt = _plate_type_from_guid(plate_type_id, cols, rows)
      plate = Plate(name=name, plate_type=pt)
      plate._id = pid
      plates.append(plate)
      if pid:
        plate_id_to_plate[pid] = plate
      group_el = pe.find(".//Group") or pe.find("Wells/Group")
      if group_el is not None:
        gid = group_el.get("ID") or group_el.get("id")
        if gid:
          group_id_to_plate[gid] = plate

  # Apply Container/Reagent data to plates (volume, reagent_name, color, reagent_type) so roundtrip preserves them.
  containers_el = protocol_el.find("Containers")
  if containers_el is not None:
    for cont in containers_el.findall("Container"):
      group_id = cont.get("groupId")
      if not group_id:
        continue
      plate = group_id_to_plate.get(group_id)
      if plate is None:
        continue
      contents = cont.find("Contents")
      if contents is None:
        continue
      reagent = contents.find("Reagent")
      if reagent is None:
        continue
      vol = reagent.get("volume")
      if vol is not None:
        try:
          plate.volume = float(vol)
        except ValueError:
          pass
      name_attr = reagent.get("name")
      if name_attr is not None:
        plate.reagent_name = name_attr if name_attr != "None" else None
      color_attr = reagent.get("color")
      if color_attr is not None:
        plate.color = color_attr
      type_attr = reagent.get("type")
      if type_attr is not None:
        plate.reagent_type = type_attr

  tips: list[Tip] = []
  steps_outer = root.find(".//Protocol/Steps")
  if steps_outer is not None:
    for tip_el in steps_outer.findall("Tip"):
      tip_name = tip_el.get("name", "Tip1")
      plates_el = tip_el.find("Plates")
      tip_plate = None
      if plates_el is not None and len(plates_el) > 0:
        first_plate_id = plates_el[0].get("id")
        if first_plate_id and first_plate_id in plate_id_to_plate:
          tip_plate = plate_id_to_plate[first_plate_id]
        else:
          tip_plate = first_plate_id or ""
      if tip_plate is None:
        tip_plate = ""
      step_els = tip_el.find("Steps")
      steps_list: list[StepT] = []
      if step_els is not None:
        for se in step_els:
          tag = _tag_local(se)
          plate_ref = None
          plates_child = se.find("Plates/Plate")
          if plates_child is not None:
            pid = plates_child.get("id", "")
            wg = plates_child.get("wellGroup", "Plate")
            if pid and pid != NULL_PLATE_ID:
              plate_ref = (pid, wg or "Plate")
          step = _parse_step(se, tag, plate_ref)
          if step is not None:
            steps_list.append(step)
      tip = Tip(name=tip_name, plate=tip_plate if tip_plate else "Placeholder", steps=steps_list)
      tips.append(tip)

  return KingFisherProtocol(name=protocol_name, plates=plates, tips=tips)


def _plate_type_from_guid(guid_str: str, columns: int, rows: int) -> PlateType:
  """Map plateTypeID + dimensions to PlateType."""
  for pt in PlateType:
    if pt.plate_type_id == guid_str:
      return pt
  if (columns, rows) == (12, 8):
    return PlateType.WP_96 if guid_str == "5d3a0092-8ae3-4d66-a17c-0ba1d569f115" else PlateType.DWP_96
  if (columns, rows) == (6, 4):
    return PlateType.DWP_24
  return PlateType.DWP_96


def _parse_collect_beads(el: ET.Element, name: str, enabled: bool, plate_ref: tuple[str, str] | None) -> CollectBeadsStep:
  count = _parse_int(el, "CollectCount", 3)
  collect_time_sec = _parse_duration_sec(el, "CollectTime", 5.0)
  return CollectBeadsStep(name=name, enabled=enabled, plate_ref=plate_ref, count=count, collect_time_sec=collect_time_sec)


def _parse_release_beads(el: ET.Element, name: str, enabled: bool, plate_ref: tuple[str, str] | None) -> ReleaseBeadsStep:
  dur_sec = _parse_duration_sec(el, "Duration", 5.0)
  speed_guid = (el.findtext("Speed") or "").strip()
  speed_name = _speed_guid_to_name(speed_guid)
  return ReleaseBeadsStep(name=name, enabled=enabled, plate_ref=plate_ref, duration_sec=dur_sec, speed=speed_name)


def _parse_dry(el: ET.Element, name: str, enabled: bool, plate_ref: tuple[str, str] | None) -> DryStep:
  dur_sec = _parse_duration_sec(el, "Duration", 300.0)
  tp = _parse_enum(el, "TipPosition", TipPosition, TipPosition.AboveSurface)
  return DryStep(name=name, enabled=enabled, plate_ref=plate_ref, duration_sec=dur_sec, tip_position=tp)


def _parse_pause(el: ET.Element, name: str, enabled: bool, plate_ref: tuple[str, str] | None) -> PauseStep:
  msg = (el.findtext("Message") or "").strip()
  disp_enabled = _parse_attr_bool(el, "Dispense", "enabled", False)
  return PauseStep(name=name, enabled=enabled, plate_ref=plate_ref, message=msg, dispense_enabled=disp_enabled)


def _parse_mix(el: ET.Element, name: str, enabled: bool, plate_ref: tuple[str, str] | None) -> MixStep:
  image = _parse_enum(el, "Image", Image, Image.Mix)
  precollect_enabled = _parse_attr_bool(el, "Precollect", "enabled", False)
  rb_enabled = _parse_attr_bool(el, "ReleaseBeads", "enabled", False)
  rb_dur = _parse_duration_sec(el, "ReleaseBeads/Duration", 0.0)
  rb_speed_raw = el.findtext("ReleaseBeads/Speed")
  rb_speed = _speed_guid_to_name((rb_speed_raw or "").strip()) if rb_speed_raw else "Fast"
  shakes: list[MixShake] = []
  for sh in el.findall("Mixing/Shakes/Shake"):
    dur = _parse_duration_to_sec(sh.get("duration", "PT0S"))
    sp = sh.get("speed", "")
    shakes.append(MixShake(duration_sec=dur, speed=_speed_guid_to_name(sp) or sp))
  loop_count = _parse_int(el, "Mixing/LoopCount", 3)
  ptp = _parse_enum(el, "Mixing/PauseTipPosition", TipPosition, TipPosition.AboveSurface)
  pause_enabled = _parse_attr_bool(el, "Pause", "enabled", False)
  pause_msg = (el.findtext("Pause/Message") or "").strip()
  heat_temp = float((el.findtext("Heating/Temperature") or "0").strip()) if el.findtext("Heating/Temperature") else 0.0
  heat_preheat = (el.findtext("Heating/Preheat") or "").strip().lower() == "true"
  postmix_enabled = _parse_attr_bool(el, "Postmix", "enabled", False)
  postmix_dur = _parse_duration_sec(el, "Postmix/Duration", 0.0)
  postmix_speed_raw = el.findtext("Postmix/Speed")
  postmix_speed = _speed_guid_to_name((postmix_speed_raw or "").strip()) or "Medium" if postmix_speed_raw else "Medium"
  cb_enabled = _parse_attr_bool(el, "CollectBeads", "enabled", True)
  cb_count = _parse_int(el, "CollectBeads/Count", 3)
  cb_time = _parse_duration_sec(el, "CollectBeads/CollectTime", 30.0)
  postt_enabled = _parse_attr_bool(el, "PostTemperature", "enabled", False)
  postt_temp = float((el.findtext("PostTemperature/Temperature") or "0").strip()) if el.findtext("PostTemperature/Temperature") else 0.0
  return MixStep(
    name=name, enabled=enabled, plate_ref=plate_ref,
    image=image, precollect_enabled=precollect_enabled,
    release_beads_enabled=rb_enabled, release_beads_duration_sec=rb_dur, release_beads_speed=rb_speed,
    shakes=shakes, loop_count=loop_count, pause_tip_position=ptp,
    pause_enabled=pause_enabled, pause_message=pause_msg,
    heating_temperature=heat_temp, heating_preheat=heat_preheat,
    postmix_enabled=postmix_enabled, postmix_duration_sec=postmix_dur, postmix_speed=postmix_speed,
    collect_beads_enabled=cb_enabled, collect_beads_count=cb_count, collect_beads_time_sec=cb_time,
    post_temperature_enabled=postt_enabled, post_temperature=postt_temp,
  )


_STEP_PARSERS: dict[str, Any] = {
  "CollectBeads": _parse_collect_beads,
  "ReleaseBeads": _parse_release_beads,
  "Dry": _parse_dry,
  "Pause": _parse_pause,
  "Mix": _parse_mix,
}


def _parse_step(el: ET.Element, tag: str, plate_ref: tuple[str, str] | None) -> StepT | None:
  parser = _STEP_PARSERS.get(tag)
  if parser is None:
    return None
  name = el.get("name", "")
  enabled = (el.get("enabled") or "true").strip().lower() == "true"
  return parser(el, name, enabled, plate_ref)


def _parse_duration_to_sec(d: str) -> float:
  """Parse XML duration (PT5S, PT1M30S) to seconds."""
  d = d.strip().upper()
  if not d.startswith("PT"):
    return 0.0
  d = d[2:]
  total = 0
  num = ""
  for c in d:
    if c in "0123456789":
      num += c
    elif c == "S":
      total += int(num or "0")
      num = ""
    elif c == "M":
      total += 60 * int(num or "0")
      num = ""
    elif c == "H":
      total += 3600 * int(num or "0")
      num = ""
  return float(total)


def _speed_guid_to_name(guid_str: str) -> str:
  for name, g in SPEED_GUIDS.items():
    if g == guid_str:
      return name
  return guid_str
