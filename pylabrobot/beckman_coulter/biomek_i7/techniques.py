"""Biomek's techniques and liquid types, translated into the arguments of the pipetting features.

The features (`head96.aspirate` / `dispense`, `pipettes.aspirate` / `dispense`) take PyLabRobot's
arguments only. Biomek keeps the same knowledge in two records of its project database:

- a **liquid type** (`Water`, `Serum`): flow rates, delays, blowout and trailing air volumes, the
  cutoff velocity, the prewet overage and the tip-touch defaults: what PyLabRobot calls a liquid
  class;
- a **technique** (`MC P1200 High`, or a method step's own `[Custom]` copy): per operation the
  height and what it is measured from (Top, Bottom or Liquid), Follow Liquid, the Z speed in the
  well and the switches (blowout, trailing air gap, tip touch, mix); in its General section the
  minimum height, conditioning, prewet, liquid-level sensing and clot detection, and its own copy
  of the liquid type's values, used instead of the liquid type's when `overrideliquidtypesettings`
  is set (every logged step on this instrument has it set, and its values are the ones logged).

This module reads both as plain records (Biomek's field names without the `c__` prefix, as the
project database stores them; `tools/export_techniques.py` writes them as JSON) and turns them into
keyword arguments:

    args = aspirate_arguments(technique, water)
    await lh.head96.aspirate(plate, 200, **{**args, "flow_rate": 50})

What each argument means, and how the pipetting template uses it, is the features' business; the
mapping is here:

| Biomek | argument |
|---|---|
| Height (mm) from Top / Bottom / Liquid | `liquid_height` (mm), `height_from` |
| Follow Liquid | `follow_liquid` |
| Minimum Height (mm above the bottom) | `minimum_height` |
| Speed (% of Z's maximum, in the well) | `z_speed` |
| aspirate / dispense speed (uL/s) | `flow_rate` |
| aspirate / dispense delay (ms) | `settling_time` (s) |
| Blowout + blowout volume | `blow_out_air_volume` (0 when off) |
| Trailing Air Gap + its volume | `transport_air_volume` (0 when off) |
| conditioning (sections, section volume, excess) | `conditioning` |
| Tip Touch + height, from, theta, speed, delay | `tip_touch` (None when off) |
| Empty Tips (dispense) | `empty` |
| cutoff velocity (uL/s), conductivity (Span-8) | `cutoff_velocity`, `lls_sensitivity` |

Mix, prewet, liquid-level sensing and clot detection map to arguments the features do not take yet;
a technique that turns them on is refused rather than run without them. `CalibrationSlope` /
`CalibrationOffset` (volume correction) are not applied: how Biomek applies them is not traced, and
a technique with other than 1 / 0 is refused.

A method's pipetting step (`tools/export_techniques.py` exports a method's steps with their
`[Custom]` techniques) adds its own height when it overrides the technique's, and names a template:
the reservoir variants of the standard templates move every aspirate 2 mm to the left (`radius
0.2` cm at `theta 180`), and the Span-8's also approaches 10 mm above the liquid.
`method_step_arguments` gives all of it, the offset as a `Coordinate` (mm).
"""

import json
import os
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple, Union

from pylabrobot.resources.coordinate import Coordinate

# What a technique's heights are measured from (Biomek's `HeightFrom`): 0 the liquid's surface,
# 1 the well's bottom, 2 its top.
HEIGHT_FROM = {0: "liquid", 1: "bottom", 2: "top"}

TECHNIQUES_ENV = "BIOMEK_I7_TECHNIQUES"


@dataclass(frozen=True)
class Conditioning:
  """Biomek's conditioning: aspirate `sections` x `section_volume` + `excess` uL more than asked,
  then dispense each section back in place before leaving the well (`MC Pipetting` / `S8
  Pipetting`'s loop), leaving the excess in the tip. The sections go back at `flow_rate` uL/s (the
  technique's dispense speed; the aspirate's when None), each followed by `delay` s."""

  sections: int = 0
  section_volume: float = 0.0
  excess: float = 0.0
  flow_rate: Optional[float] = None
  delay: float = 0.0

  @property
  def extra(self) -> float:
    return self.sections * self.section_volume + self.excess


@dataclass(frozen=True)
class TipTouch:
  """A touch-off: the tip end to `height` mm from `height_from`, out `distance` mm in the direction
  `theta` (Biomek's degrees: 90 is towards the back of the deck) and back, at `speed` % of the axes'
  maximum, then `delay` s. `distance` None goes to the container's wall at its top outline, less the
  tip's widest radius (STAR's `side_touch_off_distance` is the same kind of figure). Biomek goes to
  the wall at the touch height less its tip's cone where it crosses the rim:
  `resource_model.labware.biomek_touch_distance` gives that distance for its labware."""

  height: float
  height_from: str = "liquid"
  theta: float = 90.0
  speed: float = 100.0
  delay: float = 0.0
  distance: Optional[float] = None


def _strip(record: Mapping[str, Any]) -> Dict[str, Any]:
  """A section of a project database record: keys without `c__`, lower case; Biomek's number
  strings as numbers; the reader's `$id`s dropped."""
  out: Dict[str, Any] = {}
  for key, value in record.items():
    if key.startswith("$"):
      continue
    name = key.lower()
    name = name[3:] if name.startswith("c__") else name
    if isinstance(value, str):
      try:
        value = float(value) if "." in value else int(value)
      except ValueError:
        pass
    out[name] = value
  return out


@dataclass
class BiomekLiquidType:
  """A Biomek liquid type: its values under Biomek's names (`aspiratespeed`, `blowoutvolume`...)."""

  name: str
  values: Dict[str, Any] = field(default_factory=dict)

  @classmethod
  def from_record(cls, record: Mapping[str, Any]) -> "BiomekLiquidType":
    """From a liquid type record as `tools/biomek_tree.py` reads it."""
    values = _strip(record.get("context") or {})
    for key in ("conductivity", "cdconductivity"):
      if key in record:
        values[key] = record[key]
    return cls(name=str(record.get("name") or values.get("liquidtype") or ""), values=values)


@dataclass
class BiomekTechnique:
  """A Biomek technique: its sections under Biomek's names (`height`, `heightfrom`...)."""

  name: str
  template: str = ""
  aspirate: Dict[str, Any] = field(default_factory=dict)
  dispense: Dict[str, Any] = field(default_factory=dict)
  mix: Dict[str, Any] = field(default_factory=dict)
  general: Dict[str, Any] = field(default_factory=dict)
  context: Dict[str, Any] = field(default_factory=dict)

  @classmethod
  def from_record(cls, record: Mapping[str, Any]) -> "BiomekTechnique":
    """From a technique record as `tools/biomek_tree.py` reads it, or a method step's embedded
    copy in the same shape."""
    return cls(
      name=str(record.get("name") or ""),
      template=str(record.get("template") or ""),
      **{s: _strip(record.get(s) or {}) for s in ("aspirate", "dispense", "mix", "general")},
      context=_strip(record.get("context") or {}),
    )


def _records(path: Union[str, Path]) -> List[Dict[str, Any]]:
  with open(path, encoding="utf-8") as fh:
    data = json.load(fh)
  return list(data.values()) if isinstance(data, dict) else list(data)


def load_techniques(path: Optional[Union[str, Path]] = None) -> Dict[str, BiomekTechnique]:
  """Techniques by name from a JSON file (a list of `asdict(BiomekTechnique)`); by default the file
  `$BIOMEK_I7_TECHNIQUES` names, which holds liquid types under `liquid_types` too."""
  data = _load(path)
  return {t["name"]: BiomekTechnique(**t) for t in data.get("techniques", [])}


def load_liquid_types(path: Optional[Union[str, Path]] = None) -> Dict[str, BiomekLiquidType]:
  """Liquid types by name, from the same file as `load_techniques`."""
  data = _load(path)
  return {t["name"]: BiomekLiquidType(**t) for t in data.get("liquid_types", [])}


def save(
  path: Union[str, Path],
  techniques: List[BiomekTechnique],
  liquid_types: Optional[List[BiomekLiquidType]] = None,
) -> None:
  """Write techniques and liquid types as `load_techniques` reads them."""
  data = {
    "techniques": [asdict(t) for t in techniques],
    "liquid_types": [asdict(t) for t in liquid_types or []],
  }
  with open(path, "w", encoding="utf-8") as fh:
    json.dump(data, fh, indent=1, sort_keys=True)


def _load(path: Optional[Union[str, Path]]) -> Dict[str, Any]:
  path = path or os.environ.get(TECHNIQUES_ENV)
  if not path:
    raise ValueError(f"no techniques file given, and ${TECHNIQUES_ENV} is not set")
  with open(path, encoding="utf-8") as fh:
    return json.load(fh)  # type: ignore[no-any-return]


# -- the translation ------------------------------------------------------------------------------


def _liquid_values(technique: BiomekTechnique, liquid_type: Optional[BiomekLiquidType]) -> Dict:
  """The liquid values a step runs with: the technique's own copy when it overrides the liquid
  type (or no liquid type is given), else the liquid type's."""
  general = technique.general
  if liquid_type is None or general.get("overrideliquidtypesettings", True):
    return general
  return {**general, **liquid_type.values}


def _refuse_unbuilt(technique: BiomekTechnique, section: Mapping[str, Any]) -> None:
  general = technique.general
  unbuilt = [
    name
    for name, on in (
      ("mix", section.get("mix")),
      ("prewet", general.get("prewet") and section is technique.aspirate),
    )
    if on
  ]
  if general.get("calibrationslope", 1.0) != 1.0 or general.get("calibrationoffset", 0.0) != 0.0:
    unbuilt.append("volume calibration")
  if unbuilt:
    raise NotImplementedError(
      f"technique {technique.name!r} uses {', '.join(unbuilt)}, which the driver does not do yet"
    )


def _tip_touch(section: Mapping[str, Any], liquid: Mapping[str, Any]) -> Optional[TipTouch]:
  if not section.get("tiptouch"):
    return None
  return TipTouch(
    height=float(liquid.get("tiptouchheight", 0.0)),
    height_from=HEIGHT_FROM[int(liquid.get("tiptouchheightfrom", 0))],
    theta=float(liquid.get("tiptouchtheta", 90.0)),
    speed=float(liquid.get("tiptouchspeed", 100.0)),
    delay=float(liquid.get("tiptouchdelay", 0.0)) / 1000,
  )


def _common(section: Mapping[str, Any], general: Mapping[str, Any]) -> Dict[str, Any]:
  return {
    "liquid_height": float(section.get("height", 0.0)),
    "height_from": HEIGHT_FROM[int(section.get("heightfrom", 1))],
    "follow_liquid": bool(section.get("followliquid", False)),
    "minimum_height": float(general.get("minimumheight", 0.0)),
    "z_speed": float(section.get("speed", 100.0)),
  }


def _span8(liquid: Mapping[str, Any], general: Mapping[str, Any]) -> Dict[str, Any]:
  return {
    "cutoff_velocity": float(liquid.get("cutoffvelocity", 150.0)),
    "lls_sensitivity": int(general.get("conductivity", liquid.get("conductivity", 3500))),
  }


def aspirate_arguments(
  technique: BiomekTechnique,
  liquid_type: Optional[BiomekLiquidType] = None,
  span8: bool = False,
) -> Dict[str, Any]:
  """The aspirate arguments a technique (and liquid type) stand for; `span8` adds the Span-8's
  (`cutoff_velocity`, `lls_sensitivity`). Scalars: a Span-8 caller lists the per-channel ones.

  Raises:
    NotImplementedError: if the technique turns on what the driver does not do yet.
  """
  section, general = technique.aspirate, technique.general
  _refuse_unbuilt(technique, section)
  liquid = _liquid_values(technique, liquid_type)
  args = _common(section, general)
  args.update(
    flow_rate=float(liquid.get("aspiratespeed", 100.0)),
    settling_time=float(liquid.get("aspiratedelay", 0.0)) / 1000,
    blow_out_air_volume=float(liquid.get("blowoutvolume", 0.0)) if section.get("blowout") else 0.0,
    transport_air_volume=(
      float(liquid.get("trailingairgapvolume", 0.0)) if section.get("trailingairgap") else 0.0
    ),
    conditioning=Conditioning(
      sections=int(general.get("conditioningsectioncount", 0)),
      section_volume=float(general.get("conditioningsectionvolume", 0.0)),
      excess=float(general.get("conditioningexcess", 0.0)),
      flow_rate=float(liquid.get("dispensespeed", 100.0)),
      delay=float(liquid.get("dispensedelay", 0.0)) / 1000,
    ),
    tip_touch=_tip_touch(section, liquid),
  )
  if span8:
    args.update(_span8(liquid, general))
  return args


def dispense_arguments(
  technique: BiomekTechnique,
  liquid_type: Optional[BiomekLiquidType] = None,
  span8: bool = False,
) -> Dict[str, Any]:
  """The dispense arguments a technique (and liquid type) stand for; as `aspirate_arguments`.

  Raises:
    NotImplementedError: if the technique turns on what the driver does not do yet.
  """
  section, general = technique.dispense, technique.general
  _refuse_unbuilt(technique, section)
  liquid = _liquid_values(technique, liquid_type)
  args = _common(section, general)
  args.update(
    flow_rate=float(liquid.get("dispensespeed", 100.0)),
    settling_time=float(liquid.get("dispensedelay", 0.0)) / 1000,
    blow_out_air_volume=float(liquid.get("blowoutvolume", 0.0)) if section.get("blowout") else 0.0,
    blow_out_delay=float(liquid.get("blowoutdelay", 0.0)) / 1000,
    tip_touch=_tip_touch(section, liquid),
    empty=bool(general.get("emptytips", False)),
  )
  if span8:
    args.update(_span8(liquid, general))
  return args


# What the reservoir templates change in the standard ones, per operation (their records, project
# database: `radius 0.2` at `theta 180` on every move of the aspirate; the Span-8's approach 10 mm).
TEMPLATE_VARIANTS: Dict[str, Dict[str, Dict[str, Any]]] = {
  "MC Pipetting Reservoir with Asp Offset": {"aspirate": {"offset": (-2.0, 0.0)}},
  "S8 Pipetting Reservoir Offset Aspirate": {
    "aspirate": {"offset": (-2.0, 0.0), "approach_height": 10.0}
  },
}


def method_step_arguments(
  step: Mapping[str, Any], liquid_type: Optional[BiomekLiquidType] = None
) -> Dict[str, Any]:
  """The arguments a method's Aspirate or Dispense step stands for (a step as
  `tools/export_techniques.py` writes it: `Operation`, `Span8`, its `technique`, and `Height` /
  `HeightFrom` when it overrides the technique's): `aspirate_arguments` / `dispense_arguments`, and
  `offset` (a `Coordinate`, mm; a Span-8 caller passes it per channel as `resource_offsets`) and
  `approach_height` from its template.

  Raises:
    ValueError: for a step that is not an aspirate or a dispense.
    NotImplementedError: as `aspirate_arguments`.
  """
  operation = (step.get("Operation") or "").lower()
  if operation not in ("aspirate", "dispense"):
    raise ValueError(f"not an aspirate or dispense step: {step.get('Operation')!r}")
  technique = BiomekTechnique(**step["technique"])
  translate = aspirate_arguments if operation == "aspirate" else dispense_arguments
  args = translate(technique, liquid_type, span8=bool(step.get("Span8")))
  if step.get("OverrideHeight") or step.get("CustomHeight"):
    args["liquid_height"] = float(step["Height"])
    args["height_from"] = HEIGHT_FROM[int(step["HeightFrom"])]
  extra = TEMPLATE_VARIANTS.get(technique.template, {}).get(operation, {})
  offset: Tuple[float, float] = extra.get("offset", (0.0, 0.0))
  args["offset"] = Coordinate(offset[0], offset[1], 0)
  if "approach_height" in extra:
    args["approach_height"] = extra["approach_height"]
  return args


def load_method_steps(method: str, path: Optional[Union[str, Path]] = None) -> List[Dict[str, Any]]:
  """A method's exported steps (`tools/export_techniques.py`), by its folder and name (
  `production\\Dilutions_96to96_ByRow`), from `path` or `$BIOMEK_I7_TECHNIQUES`.

  Raises:
    KeyError: for a method the file does not hold.
  """
  return list(_load(path)["method_steps"][method]["steps"])


def for_channels(args: Mapping[str, Any], channels: int) -> Dict[str, Any]:
  """`args` for a Span-8's `aspirate` / `dispense`, which take some arguments per channel: the same
  `liquid_height`, `flow_rate` and `offset` on each of `channels` channels (as `liquid_heights`,
  `flow_rates`, `resource_offsets`)."""
  out = dict(args)
  for one, many in (
    ("liquid_height", "liquid_heights"),
    ("flow_rate", "flow_rates"),
    ("offset", "resource_offsets"),
  ):
    if one in out:
      out[many] = [out.pop(one)] * channels
  return out
