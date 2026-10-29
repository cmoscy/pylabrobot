"""Biomek i7 pods and their bridges.

Each pod hangs from a bridge that spans the deck from front to back and rides the X rails; the pod
travels along its bridge in Y, and its head in Z. On this unit pod 1 carries the 96-channel
Multichannel head and pod 2 the Span-8, each with a gripper.

Sized in centimetres. The spans the collision model uses (BridgeXSpan,
BridgeToHeadXOffset, PodXSpan, PodYSpan) are the instrument's own when its declared configuration
is given (`pod_model`), so the model's X extents are the ones the rail guard computes
(`driver/pod_geometry.py`); otherwise the pod type's defaults under
`HardwareSetupInfo/PodTypes/<MC|Span8>`. A value marked `assumed` has no source yet.

Along X, as Biomek's collision model places them: the bridge's left edge is BridgeToHeadXOffset left of the
pipettor's (`bridge_x_range`, `pipettor_x_range` without the avoidance margin), and the pipettor
is the carriage, PodXSpan wide.

A pod is its carriage: its frame is the carriage's front left bottom corner, and the head and the
gripper hang below it at negative z, so moving the pod moves everything on it.
"""

from typing import Dict, Optional, Tuple

from pylabrobot.beckman_coulter.biomek_i7.driver.configuration import PodGeometry
from pylabrobot.resources.coordinate import Coordinate
from pylabrobot.resources.resource import Resource

from .units import cm

# Both pod types. Biomek_i7.bif HardwareSetupInfo/PodTypes/<MC|Span8>: BridgeXSpan,
# BridgeToHeadXOffset
BRIDGE_X_SPAN = 12.7
BRIDGE_TO_HEAD_X_OFFSET = 2.0

# Pod 1, the 96-channel Multichannel pod. PodTypes/MC: PodXSpan, PodYSpan, HeadBodyXSpan,
# HeadBodyYSpan, HeadHeight, HeadFlangeHeight, PodToHeadBodyXOffset, PodToHeadBodyYOffset
POD_96_X_SPAN = 18.606
POD_96_Y_SPAN = 11.43
HEAD_BODY_X_SPAN = 13.0175
HEAD_BODY_Y_SPAN = 7.9375
HEAD_HEIGHT = 9.8171
HEAD_FLANGE_HEIGHT = 1.0795
POD_TO_HEAD_BODY_X_OFFSET = 2.795
POD_TO_HEAD_BODY_Y_OFFSET = 1.74625

# Pod 2, the Span-8 pod. PodTypes/Span8: PodXSpan, PodYSpan
POD_SPAN8_X_SPAN = 4.0
POD_SPAN8_Y_SPAN = 10.0

# Gripper fingers, the same on both pods. PodTypes/<MC|Span8>/GripperModel/Finger:
# FingerMountOriginSpanAtHardstop, BottomPart/FingerwiseSpan; Finger/FingerMountOriginZSpan
FINGER_MOUNT_SPAN_AT_HARDSTOP = 6.321
FINGER_BOTTOM_SPAN = 9.421
FINGER_MOUNT_Z_SPAN = 1.397

# assumed: the bridge's section, the carriage's height, the Span-8 channel block's height, the
# gripper body and a finger's section. Not yet sourced; the simulator holds them as constants.
BRIDGE_HEIGHT = 10.0
POD_BODY_HEIGHT = 25.0
SPAN8_BLOCK_HEIGHT = 12.0
GRIPPER_BODY = (3.0, 8.0, 4.0)
FINGER_THICKNESS = 0.6

_COLORS: Dict[str, str] = {
  "x_arm": "#6b7a85",
  "arm": "#c9ced3",
  "head_body": "#3d4f63",
  "head_flange": "#8a96a3",
}


# Model names, namespaced: the viewer draws a resource from `<model>.glb` under the package.
MODEL_HEAD_96 = "beckman_coulter_biomek_i7_head_96"
MODEL_SPAN8_BLOCK = "beckman_coulter_biomek_i7_span8_block"


def _part(
  name: str,
  size_x: float,
  size_y: float,
  size_z: float,
  category: str,
  model: Optional[str] = None,
) -> Resource:
  """A box of a pod, every length in Biomek's centimetres."""
  part = Resource(
    name=name,
    size_x=cm(size_x),
    size_y=cm(size_y),
    size_z=cm(size_z),
    category=category,
    model=model,
  )
  if category in _COLORS:
    # A field the viewer reads off the resource; `Resource` does not declare it.
    part.appearance = {"color": _COLORS[category]}  # type: ignore[attr-defined]
  return part


def _gripper(name: str) -> Resource:
  """A pod's gripper: a body with two fingers below it, open at the hardstop, in its own frame."""
  body_x, body_y, body_z = GRIPPER_BODY
  body = _part(name, body_x, body_y, body_z, "body")
  # The fingers reach FingerwiseSpan below their mount, which sits FingerMountOriginZSpan into the
  # body, and stand FingerMountOriginSpanAtHardstop apart in Y, centred on it.
  finger_z = FINGER_BOTTOM_SPAN
  for i, side in enumerate((-1, 1)):
    y = body_y / 2 + side * FINGER_MOUNT_SPAN_AT_HARDSTOP / 2 - FINGER_THICKNESS / 2
    body.assign_child_resource(
      _part(f"{name}_finger_{i}", FINGER_THICKNESS, FINGER_THICKNESS, finger_z, "finger"),
      location=Coordinate(
        cm(body_x / 2 - FINGER_THICKNESS / 2), cm(y), cm(FINGER_MOUNT_Z_SPAN - finger_z)
      ),
    )
  return body


def _mount_gripper(pod: Resource, pod_x_span: float, pod_y_span: float) -> None:
  """Hang the gripper off the pod's right face, its body's top level with the carriage's bottom.

  Where it mounts is assumed until the simulator's assembly is read.
  """
  body_y, body_z = GRIPPER_BODY[1], GRIPPER_BODY[2]
  pod.assign_child_resource(
    _gripper(f"{pod.name}_gripper"),
    location=Coordinate(cm(pod_x_span), cm((pod_y_span - body_y) / 2), -cm(body_z)),
  )


def pod_96(
  name: str = "left_pod", pod_x_span: float = POD_96_X_SPAN, pod_y_span: float = POD_96_Y_SPAN
) -> Resource:
  """The Multichannel pod: the 96-channel head and its gripper, under its carriage (cm spans)."""
  # An arm: the viewer draws it as an open frame, so the deck shows through it.
  pod = _part(name, pod_x_span, pod_y_span, POD_BODY_HEIGHT, "arm")
  head_x, head_y = cm(POD_TO_HEAD_BODY_X_OFFSET), cm(POD_TO_HEAD_BODY_Y_OFFSET)
  pod.assign_child_resource(
    _part(
      f"{name}_head", HEAD_BODY_X_SPAN, HEAD_BODY_Y_SPAN, HEAD_HEIGHT, "head_body", MODEL_HEAD_96
    ),
    location=Coordinate(head_x, head_y, -cm(HEAD_HEIGHT)),
  )
  pod.assign_child_resource(
    _part(
      f"{name}_head_flange", HEAD_BODY_X_SPAN, HEAD_BODY_Y_SPAN, HEAD_FLANGE_HEIGHT, "head_flange"
    ),
    location=Coordinate(head_x, head_y, -cm(HEAD_HEIGHT + HEAD_FLANGE_HEIGHT)),
  )
  _mount_gripper(pod, pod_x_span, pod_y_span)
  return pod


def pod_span8(
  name: str = "right_pod",
  pod_x_span: float = POD_SPAN8_X_SPAN,
  pod_y_span: float = POD_SPAN8_Y_SPAN,
) -> Resource:
  """The Span-8 pod: its channel block and its gripper, under its carriage (cm spans)."""
  pod = _part(name, pod_x_span, pod_y_span, POD_BODY_HEIGHT, "arm")
  pod.assign_child_resource(
    _part(
      f"{name}_channel_block",
      pod_x_span,
      pod_y_span,
      SPAN8_BLOCK_HEIGHT,
      "head_body",
      MODEL_SPAN8_BLOCK,
    ),
    location=Coordinate(0, 0, -cm(SPAN8_BLOCK_HEIGHT)),
  )
  _mount_gripper(pod, pod_x_span, pod_y_span)
  return pod


def bridge(
  name: str,
  pod: Resource,
  length: float,
  pod_y: float,
  bridge_x_span: float = BRIDGE_X_SPAN,
  bridge_to_head_x_offset: float = BRIDGE_TO_HEAD_X_OFFSET,
) -> Resource:
  """A bridge `length` mm long front to back, carrying `pod` at `pod_y` mm from its front end.

  Args:
    name: what to call the bridge.
    pod: the pod it carries, hung below it.
    length: how far it spans, in mm: the deck's depth.
    pod_y: where the pod's front edge is along the bridge, in mm.
    bridge_x_span: the bridge's width, cm (BridgeXSpan).
    bridge_to_head_x_offset: how far right of the bridge's left edge the pod's carriage begins,
      cm (BridgeToHeadXOffset).
  """
  rail = Resource(
    name=name,
    size_x=cm(bridge_x_span),
    size_y=length,
    size_z=cm(BRIDGE_HEIGHT),
    # The viewer draws an x_arm as an open frame riding the rails, so the deck shows through it.
    category="x_arm",
  )
  rail.appearance = {"color": _COLORS["x_arm"]}  # type: ignore[attr-defined]
  rail.assign_child_resource(
    pod, location=Coordinate(cm(bridge_to_head_x_offset), pod_y, -pod.get_size_z())
  )
  return rail


def pod_model(
  side: str, module_type: str, length: float, geometry: Optional[PodGeometry] = None
) -> Tuple[Resource, Resource]:
  """A pod and the bridge carrying it, named `<side>_pod` and `<side>_bridge`: from the pod's
  declared `geometry` when given, else the pod type's defaults. Returns (bridge, pod)."""
  builders = {"multichannel": pod_96, "span8": pod_span8}
  if module_type not in builders:
    raise ValueError(f"no model for a {module_type!r} pod; have {sorted(builders)}")
  if geometry is None:
    pod = builders[module_type](f"{side}_pod")
    return bridge(f"{side}_bridge", pod, length, 0.0), pod
  pod = builders[module_type](f"{side}_pod", geometry.pod_x_span, geometry.pod_y_span)
  rail = bridge(
    f"{side}_bridge",
    pod,
    length,
    0.0,
    geometry.bridge_x_span,
    geometry.bridge_to_head_x_offset,
  )
  return rail, pod
