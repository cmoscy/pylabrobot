"""The Span-8's liquid-level sensing on the simulator: an aspirate, a dispense and a probe that
search for the liquid first, as Biomek's logs show it (2026-11-25, a sense in a 96-well plate
before an aspirate with conductive tips), with STAR's arguments (`lld_mode`, `immersion_depths`,
`search_speed`, `probe_liquid_heights`)."""

from typing import Dict, List, Optional

from pylabrobot.beckman_coulter.biomek_i7.driver import liquid
from pylabrobot.beckman_coulter.biomek_i7.driver import protocol as p
from pylabrobot.beckman_coulter.biomek_i7.driver.liquid_tests import LiquidTest
from pylabrobot.beckman_coulter.biomek_i7.driver.simulator import simulate_liquid
from pylabrobot.beckman_coulter.biomek_i7.resource_model.labware import biomek_tip
from pylabrobot.beckman_coulter.biomek_i7 import units
from pylabrobot.beckman_coulter.biomek_i7.simulation_tests_support import generic_pods
from pylabrobot.beckman_coulter.biomek_i7.techniques import (
  BiomekTechnique,
  LiquidSearch,
  LLDMode,
  aspirate_arguments,
)
from pylabrobot.beckman_coulter.biomek_i7.units import field_to_rate
from pylabrobot.resources import set_volume_tracking

ROWS = "ABCDEFGH"


class LiquidLevelSensingTests(LiquidTest):
  async def sensing_deck(self, volumes: List[float], tip_class: str = "T190F_LLS"):  # type: ignore[no-untyped-def]
    driver, lh = await self.setup_deck()
    simulate_liquid(driver, driver.boards)
    pipettes = lh.pipettes
    tips = {n: biomek_tip(tip_class, f"tip {n}") for n in range(1, len(volumes) + 1)}
    pipettes._set_tips(tips)
    pipettes._tips.update(tips)
    plate = lh.deck.get_resource("Plate")
    wells = [plate.get_well(f"{ROWS[i]}1") for i in range(len(volumes))]
    for well, volume in zip(wells, volumes):
      well.tracker.set_volume(volume)
    return driver, lh, wells, tips

  def requests(self, driver) -> List[str]:  # type: ignore[no-untyped-def]
    """The Span-8 board's requests since the mark, by name."""
    return [name for name, _ in self.sent(driver, "span8")]

  async def test_an_aspirate_senses_then_goes_under_each_probes_own_surface(self) -> None:
    # As Biomek's logs show (2026-11-25): the approach, the blowout air, the sensors' sensitivity
    # once, the probes to the search's start at the technique's Z speed (5 %), the search at 20 %
    # with the LLS/CD sensor, then each probe to 2 mm under the surface it found, at the Z speed,
    # and the aspirate. The probes' positions are read back before that move.
    volumes = [199.5, 195.1, 229.3, 183.3, 201.5, 206.1, 210.7, 155.5]
    driver, lh, wells, tips = await self.sensing_deck(volumes)
    pipettes = lh.pipettes
    pod = self.pod(driver, "right")
    declared = pod.declared
    assert declared is not None
    z_speed = field_to_rate(pod, "Z1", 50)
    moves = self.record(driver, "right")
    self.mark(driver)
    await pipettes.aspirate(
      wells, [50] * 8, lld_mode=LLDMode.CAPACITIVE, z_speed=z_speed, blow_out_air_volumes=[30] * 8,
      follow_liquid=True,
    )  # fmt: skip
    after = self.after_travel(moves)
    speeds = [speed for _, speed in after]
    self.assertEqual(speeds[:6], [1000, speeds[1], 50, 200, 50, speeds[5]])
    approach, blowout, start, search, down, draw = after[:6]
    self.assertEqual(set(blowout[0]), {f"D{n}" for n in range(1, 9)})
    self.assertEqual(set(search[0]), {f"Z{n}" for n in range(1, 9)})
    # Each probe goes 2 mm under its own surface: the level its well's contents stand at.
    plan = pipettes.plan_liquid("aspirate", wells, [50] * 8, None, pipettes._settings(), 90)[0]
    for c, well, volume in zip(plan.channels, wells, volumes):
      surface = c.bottom + c.tip_length + well.compute_height_from_volume(volume) / 10
      self.assertAlmostEqual(down[0][c.name], declared.to_counts(c.name, surface - 0.2), delta=1)
      self.assertLess(start[0][c.name], declared.to_counts(c.name, surface + 1.0))
    # One `SS`, before the search; the search's stops read back before the move under the surface.
    names = self.requests(driver)
    self.assertEqual(names.count("Set LLS/CD Sensitivity"), 1)
    sense = names.index("Set LLS/CD Sensitivity")
    stop = names.index("Stop Motion Service", sense)
    self.assertEqual(names[stop + 1 : stop + 17].count("Get Current Position"), 8)
    # The wells hold what was found less what was drawn; the tips what was drawn.
    for well, volume, n in zip(wells, volumes, range(1, 9)):
      self.assertAlmostEqual(well.tracker.get_used_volume(), volume - 50, delta=0.05)
      self.assertAlmostEqual(tips[n].tracker.get_used_volume(), 50)

  async def finished_lift(self, n: int, end: Optional[float] = None) -> Dict[str, int]:
    """Where a sensed aspirate that succeeds leaves `n` probes: its last move (the lift)."""
    driver, lh, wells, _ = await self.sensing_deck([120.0] * n)
    moves = self.record(driver, "right")
    await lh.pipettes.aspirate(
      wells, [20] * n, lld_mode=LLDMode.CAPACITIVE, minimum_traverse_height_end=end
    )
    return {f"Z{k}": moves[-1][0][f"Z{k}"] for k in range(1, n + 1)}

  async def test_no_liquid_found_raises_with_the_probes_where_a_finished_aspirate_leaves_them(
    self,
  ) -> None:
    # Biomek leaves the probes at the search's floor for its recovery; the driver raises, so it
    # lifts them first, as a finished operation does (to `minimum_traverse_height_end`).
    for end in (None, 250.0):
      with self.subTest(minimum_traverse_height_end=end):
        driver, lh, wells, tips = await self.sensing_deck([120.0, 0.0])
        moves = self.record(driver, "right")
        with self.assertRaisesRegex(RuntimeError, r"no liquid found under probe\(s\) \[2\]"):
          await lh.pipettes.aspirate(
            wells, [20, 20], lld_mode=LLDMode.CAPACITIVE, minimum_traverse_height_end=end
          )
        last, speed = moves[-1]
        self.assertEqual({a: last[a] for a in ("Z1", "Z2")}, await self.finished_lift(2, end))
        self.assertEqual(speed, 1000)
        # Nothing drawn, nothing booked.
        self.assertAlmostEqual(wells[0].tracker.get_used_volume(), 120.0)
        self.assertEqual([tips[n].tracker.get_used_volume() for n in (1, 2)], [0, 0])

  async def test_less_liquid_than_to_aspirate_raises_and_keeps_what_was_found(self) -> None:
    driver, lh, wells, tips = await self.sensing_deck([30.0])
    with self.assertRaisesRegex(RuntimeError, "less than the 50.00 uL to aspirate"):
      await lh.pipettes.aspirate(wells, [50], lld_mode=LLDMode.CAPACITIVE)
    self.assertAlmostEqual(wells[0].tracker.get_used_volume(), 30.0, delta=0.05)
    self.assertEqual(tips[1].tracker.get_used_volume(), 0)

  async def test_sensing_needs_conductive_tips_and_refuses_before_moving(self) -> None:
    driver, lh, wells, _ = await self.sensing_deck([100.0], tip_class="T190F")
    moves = self.record(driver, "right")
    with self.assertRaisesRegex(ValueError, "conductive tips"):
      await lh.pipettes.aspirate(wells, [20], lld_mode=LLDMode.CAPACITIVE)
    self.assertEqual(moves, [])

  async def test_the_arguments_that_go_with_sensing(self) -> None:
    driver, lh, wells, _ = await self.sensing_deck([100.0])
    pipettes = lh.pipettes
    moves = self.record(driver, "right")
    refused: List[Dict[str, object]] = [
      {"lld_mode": LLDMode.CAPACITIVE, "liquid_heights": [1.0]},  # the search finds the surface
      {"lld_mode": LLDMode.CAPACITIVE, "height_from": "bottom"},
      {"lld_mode": "PRESSURE"},  # the Span-8 senses by capacitance only
      {"immersion_depths": [2.0]},  # sensing's arguments without sensing
      {"lld_mode": LLDMode.CAPACITIVE, "liquid_search": LiquidSearch(start_from="liquid")},
    ]
    for kwargs in refused:
      with self.subTest(**kwargs), self.assertRaises(ValueError):
        await pipettes.aspirate(wells, [20], **kwargs)  # type: ignore[arg-type]
    self.assertEqual(moves, [])

  async def test_a_dispense_senses_too_and_refuses_to_overfill(self) -> None:
    driver, lh, wells, tips = await self.sensing_deck([100.0, 100.0])
    pipettes = lh.pipettes
    await pipettes.aspirate(wells, [40, 40], lld_mode=LLDMode.CAPACITIVE)
    await pipettes.dispense(wells, [40, 40], lld_mode=LLDMode.CAPACITIVE, immersion_depths=[1.0])
    for well in wells:
      self.assertAlmostEqual(well.tracker.get_used_volume(), 100.0, delta=0.1)
    await pipettes.aspirate(wells, [40, 40], lld_mode=LLDMode.CAPACITIVE)
    wells[0].tracker.set_volume(wells[0].max_volume - 10)
    with self.assertRaisesRegex(RuntimeError, "would overfill"):
      await pipettes.dispense(wells, [40, 40], lld_mode=LLDMode.CAPACITIVE)
    self.assertEqual([tips[n].tracker.get_used_volume() for n in (1, 2)], [40, 40])

  async def test_probing_reports_heights_and_volumes_and_books_nothing(self) -> None:
    driver, lh, wells, _ = await self.sensing_deck([150.0, 0.0, 80.0])
    pipettes = lh.pipettes
    heights = await pipettes.probe_liquid_heights(wells, n_replicates=2)
    self.assertAlmostEqual(heights[0], wells[0].compute_height_from_volume(150.0), delta=0.01)
    self.assertEqual(heights[1], 0.0)
    self.assertAlmostEqual(heights[2], wells[2].compute_height_from_volume(80.0), delta=0.01)
    volumes = await pipettes.probe_liquid_volumes(wells)
    self.assertEqual([round(v, 1) for v in volumes], [150.0, 0.0, 80.0])
    self.assertEqual([w.tracker.get_used_volume() for w in wells], [150.0, 0.0, 80.0])
    with self.assertRaises(ValueError):
      await pipettes.probe_liquid_heights(wells, lld_mode=LLDMode.OFF)

  async def test_without_tracking_the_simulator_senses_nothing(self) -> None:
    driver, lh, wells, _ = await self.sensing_deck([150.0])
    set_volume_tracking(False)
    self.assertEqual(await lh.pipettes.probe_liquid_heights(wells), [0.0])

  async def test_the_plan_stops_where_the_level_is_first_needed(self) -> None:
    driver, lh, wells, _ = await self.sensing_deck([100.0])
    pipettes = lh.pipettes
    settings = pipettes._settings(
      liquid_height=-2.0, height_from="liquid", blow_out_air_volume=10.0,
      search=liquid.Search(),
    )  # fmt: skip
    aspirate = pipettes.plan_liquid("aspirate", wells, [20], None, settings, 90)[0]
    self.assertEqual(
      [type(s).__name__ for s in aspirate.steps], ["ZMove", "PlungerMove"]
    )  # the approach, the blowout air
    assert aspirate.seek is not None
    dispense = pipettes.plan_liquid("dispense", wells, [20], None, settings, 90)[0]
    self.assertEqual([type(s).__name__ for s in dispense.steps], ["ZMove"])  # the approach
    # The search starts at the top (Biomek's default, 0 mm from it) and goes at most to the
    # bottom.
    c = aspirate.channels[0]
    self.assertAlmostEqual(aspirate.seek.start[c.name], c.bottom + c.tip_length + c.well.depth)
    self.assertAlmostEqual(aspirate.seek.floor[c.name], c.bottom + c.tip_length)
    self.assertEqual((aspirate.seek.speed, p.LLS_CD_SENSOR), (200, 15))

  def record_sensing(self, driver):  # type: ignore[no-untyped-def]
    """Each move the Span-8 pod is asked for: (targets, speed, whether its sensors are armed)."""
    pod = self.pod(driver, "right")
    moves = []
    original = pod.move_axes

    async def move_axes(targets, speed=1000, **kwargs):  # type: ignore[no-untyped-def]
      moves.append((dict(targets), speed, bool(kwargs.get("sensors"))))
      return await original(targets, speed, **kwargs)

    pod.move_axes = move_axes  # type: ignore[method-assign, assignment]
    return moves

  def count_searches(self, moves) -> int:  # type: ignore[no-untyped-def]
    return sum(1 for _, _, sensed in moves if sensed)

  async def test_repeated_searches_stop_early_when_every_probe_agrees(self) -> None:
    driver, lh, wells, _ = await self.sensing_deck([150.0, 120.0])
    moves = self.record_sensing(driver)
    search = LiquidSearch(repeats=3, accept_tolerance=0.5)
    await lh.pipettes.probe_liquid_heights(wells, liquid_search=search)
    self.assertEqual(self.count_searches(moves), 2)  # the second agrees with the first
    moves.clear()
    await lh.pipettes.probe_liquid_heights(wells, liquid_search=LiquidSearch(repeats=3))
    self.assertEqual(self.count_searches(moves), 3)  # without a tolerance, every search is made

  async def test_an_unsteady_level_is_refused_after_the_last_search(self) -> None:
    driver, lh, wells, _ = await self.sensing_deck([150.0])
    hook = driver.simulated_liquid
    assert hook is not None
    rounds = []

    def rising(arm, levels):  # type: ignore[no-untyped-def]
      # Each search finds the surface 40 counts higher (about 0.1 mm), as liquid that settles.
      rounds.append(1)
      hook(arm, {a: None if at is None else at + 40 * len(rounds) for a, at in levels.items()})

    driver.simulated_liquid = rising
    moves = self.record_sensing(driver)
    with self.assertRaisesRegex(RuntimeError, "not steady"):
      await lh.pipettes.aspirate(
        wells, [20], lld_mode=LLDMode.CAPACITIVE,
        liquid_search=LiquidSearch(repeats=2, fail_tolerance=0.01),
      )  # fmt: skip
    self.assertEqual(self.count_searches(moves), 2)
    self.assertEqual(moves[-1][0]["Z1"], (await self.finished_lift(1))["Z1"])
    self.assertAlmostEqual(wells[0].tracker.get_used_volume(), 150.0)  # nothing booked

  async def test_searching_from_where_the_probes_stand(self) -> None:
    driver, lh, wells, _ = await self.sensing_deck([150.0])
    moves = self.record_sensing(driver)
    await lh.pipettes.probe_liquid_heights(wells, liquid_search=LiquidSearch(move_to_start=False))
    search = next(i for i, (_, _, sensed) in enumerate(moves) if sensed)
    self.assertEqual(moves[search - 1][1], 1000)  # straight from the approach (full speed)
    moves.clear()
    await lh.pipettes.probe_liquid_heights(wells)
    search = next(i for i, (_, _, sensed) in enumerate(moves) if sensed)
    self.assertEqual(moves[search - 1][1], lh.pipettes._settings().z_speed_field)  # to the start

  def test_a_techniques_sensing_settings(self) -> None:
    # "S8 1000 Medium" as exported from the project database, its Sense count raised to 3 with
    # both tolerances on.
    technique = BiomekTechnique(
      name="S8 1000 Medium",
      aspirate={"height": -2.0, "heightfrom": 0, "speed": 5.0, "followliquid": True},
      general={
        "minimumheight": 1.5, "detectionspeed": 20.0, "llsinitial": True, "llsinitialheight": 0,
        "llsinitialheightfrom": 2, "llsrepeat": 3, "llsaccept": True, "llsacceptvalue": 0.5,
        "llsfail": True, "llsfailvalue": 2.0, "conductivity": "3500",
      },
    )  # fmt: skip
    _, span8 = generic_pods()
    args = aspirate_arguments(technique, arm=span8, lld=True)
    self.assertEqual(args["lld_mode"], LLDMode.CAPACITIVE)
    self.assertEqual(args["immersion_depth"], 2.0)
    self.assertNotIn("liquid_height", args)
    self.assertEqual(units.rate_to_field(span8, "Z1", args["search_speed"]), 200)
    self.assertEqual(
      args["liquid_search"],
      LiquidSearch(start_height=0.0, start_from="top", repeats=3, accept_tolerance=0.5,
                   fail_tolerance=2.0),
    )  # fmt: skip
    self.assertNotIn("lld_mode", aspirate_arguments(technique, arm=span8))
    off = BiomekTechnique(technique.name, aspirate=technique.aspirate,
                          general={**technique.general, "llsrepeat": 0})  # fmt: skip
    with self.assertRaisesRegex(ValueError, "Sense 0"):
      aspirate_arguments(off, arm=span8, lld=True)
    from_liquid = BiomekTechnique(technique.name, aspirate=technique.aspirate,
                                  general={**technique.general, "llsinitialheightfrom": 0})  # fmt: skip
    with self.assertRaises(NotImplementedError):
      aspirate_arguments(from_liquid, arm=span8, lld=True)
