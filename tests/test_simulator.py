"""
Unit tests for MarineEngineSimulator.

Verifies the physical integrity of generated data: parameter ranges,
fault injection correctness, dataset length, fault rate accuracy,
and deterministic reproducibility via seed.
"""
import pytest
import pandas as pd
from src.simulator.engine_simulator import MarineEngineSimulator


@pytest.fixture
def simulator():
    """Return a seeded MarineEngineSimulator for deterministic test results."""
    return MarineEngineSimulator(seed=42)


class TestSimulator:
    """Tests for data generation, fault injection, and reproducibility."""

    def test_normal_reading_has_all_parameters(self, simulator):
        """A normal reading must contain every required sensor field."""
        reading = simulator.generate_normal_reading()
        required = [
            "rpm", "temperature_exhaust", "temperature_cooling",
            "pressure_lube", "pressure_fuel", "vibration_rms",
            "timestamp", "fault_injected"
        ]
        for param in required:
            assert param in reading, f"Missing parameter: {param}"

    def test_normal_reading_within_ranges(self, simulator):
        """All normal readings must stay within the simulator's defined ranges."""
        for _ in range(100):
            reading = simulator.generate_normal_reading()
            for param, (low, high) in simulator.NORMAL_RANGES.items():
                assert low <= reading[param] <= high, \
                    f"{param}={reading[param]} out of range [{low}, {high}]"

    def test_fault_injection_overheating(self, simulator):
        """Overheating fault must push exhaust temperature above 400 °C."""
        reading = simulator.generate_normal_reading()
        faulted = simulator.inject_fault(reading.copy(), "overheating")
        assert faulted["temperature_exhaust"] > 400.0
        assert faulted["fault_injected"] is True

    def test_fault_injection_low_lube_pressure(self, simulator):
        """Low lube pressure fault must drop oil pressure below 3.0 bar."""
        reading = simulator.generate_normal_reading()
        faulted = simulator.inject_fault(reading.copy(), "low_lube_pressure")
        assert faulted["pressure_lube"] < 3.0
        assert faulted["fault_injected"] is True

    def test_fault_injection_high_vibration(self, simulator):
        """High vibration fault must raise RMS vibration above 5.0 mm/s."""
        reading = simulator.generate_normal_reading()
        faulted = simulator.inject_fault(reading.copy(), "high_vibration")
        assert faulted["vibration_rms"] > 5.0
        assert faulted["fault_injected"] is True

    def test_unknown_fault_type_does_not_crash(self, simulator):
        """An unrecognised fault type must be handled gracefully without raising."""
        reading = simulator.generate_normal_reading()
        result  = simulator.inject_fault(reading.copy(), "unknown_fault")
        assert result is not None

    def test_dataset_has_correct_length(self, simulator):
        """Dataset length must equal hours × (3600 / interval_seconds)."""
        df = simulator.generate_dataset(hours=1, interval_seconds=60)
        assert len(df) == 60

    def test_dataset_fault_rate_approximate(self, simulator):
        """Observed fault rate must be within ±3 pp of the configured probability."""
        df = simulator.generate_dataset(
            hours=24,
            fault_probability=0.05
        )
        actual_rate = df["fault_injected"].mean()
        assert 0.02 <= actual_rate <= 0.08, \
            f"Fault rate {actual_rate:.3f} too far from 0.05"

    def test_same_seed_produces_same_data(self):
        """Two simulators with the same seed must produce identical sensor readings.

        The timestamp column is excluded from the comparison because it is
        generated from the system clock and cannot be controlled by the seed.
        Reproducibility applies only to the sensor values and fault flags.
        """
        sim1 = MarineEngineSimulator(seed=99)
        sim2 = MarineEngineSimulator(seed=99)
        df1  = sim1.generate_dataset(hours=1)
        df2  = sim2.generate_dataset(hours=1)
        pd.testing.assert_frame_equal(
            df1.drop(columns=["timestamp"]),
            df2.drop(columns=["timestamp"])
        )
