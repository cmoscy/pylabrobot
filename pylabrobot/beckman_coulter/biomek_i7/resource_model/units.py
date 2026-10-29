"""Biomek works in centimetres; PyLabRobot works in millimetres. The one place the two meet."""

MM_PER_CM = 10.0


def cm(value: float) -> float:
  """A length Biomek states in centimetres, in millimetres.

  Every dimension transcribed from a Biomek definition goes through here, written as Biomek states
  it, so the literal in the code can be compared against its source as it stands.
  """
  return value * MM_PER_CM
