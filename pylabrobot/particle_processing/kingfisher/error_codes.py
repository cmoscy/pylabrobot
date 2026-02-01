"""KingFisher Presto error and warning codes from Interface Specification section 4.8, 4.9.

Used to make PrestoConnectionError and get_status() error_code_description more informative.
"""

from typing import Optional

# Error codes (Res/Error@code, Evt/Error@code). Spec 4.8.
ERROR_CODES: dict[int, str] = {
  2: "Received an unknown command.",
  3: "Already connected to another port.",
  4: "Head position error.",
  5: "Magnets position error.",
  6: "Turntable position error.",
  7: "Heater unit position error.",
  8: "Lock position error.",
  11: "Invalid command argument.",
  13: "Protocol memory error.",
  14: "Protocol memory is full.",
  15: "No protocols found from the protocols memory.",
  16: "Protocol was not found from the protocols memory.",
  17: "Given tip name was not found from the protocol.",
  18: "Given step name was not found from the given tip of the protocol.",
  19: "A name of a step to start was not given.",
  20: "A name of a tip where to start the step was not given.",
  23: "Protocol name is invalid. Maximum length of the name is 100 bytes e.g. 100 ASCII characters.",
  24: "Invalid protocol file.",
  25: "Protocol is not executable.",
  27: "Protocol is too large and can't be loaded.",
  28: "Instrument is executing, please wait.",
  32: "No protocol is currently running.",
  33: "Data transmit to USB port failed (timed out).",
  34: "Cannot run magnets down without tips.",
  35: "Magnetic head is missing.",
  38: "Plate not detected in processing position.",
  124: "Protocol already running.",
  321: "Execution failed.",
}

# Warning codes (Res/Warning@code). Spec 4.9. (Only 101 confirmed from spec; add more as documented.)
WARNING_CODES: dict[int, str] = {
  101: "Instrument is already connected.",
}


def get_error_code_description(code: int) -> Optional[str]:
  """Return the standard error description for a code, or None if unknown."""
  return ERROR_CODES.get(code)


def get_warning_code_description(code: int) -> Optional[str]:
  """Return the standard warning description for a code, or None if unknown."""
  return WARNING_CODES.get(code)


def format_error_message(
  code: Optional[int],
  instrument_text: Optional[str],
  *,
  kind: str = "error",
) -> str:
  """Build an informative message from code and/or instrument text.

  Prefers instrument text when present; appends standard description when we have a known code.
  kind is \"error\" or \"warning\" (only error codes are in our table for now; warnings use same logic).
  """
  if instrument_text and instrument_text.strip():
    text = instrument_text.strip()
  else:
    text = None
  desc = get_error_code_description(code) if code is not None else None
  if kind == "warning" and code is not None:
    desc = get_warning_code_description(code) or desc
  if desc and text and desc != text:
    return f"{desc} ({text})"
  if desc:
    return desc
  if text:
    return text
  if code is not None:
    return f"Unknown {'warning' if kind == 'warning' else 'error'} code {code}."
  return "Command failed"
