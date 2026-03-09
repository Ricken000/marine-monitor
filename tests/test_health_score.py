"""
Unit tests for EngineHealthScorer.

Verifies correct behaviour under normal, boundary, and extreme operating
conditions, including score bounds, status label assignment, and DataFrame
immutability guarantees.
"""
import pytest
import pandas as pd
from src.analysis.health_score import EngineHealthScorer


@pytest.fixture
def scorer():
    """Return a default EngineHealthScorer instance."""
    return EngineHealthScorer()


@pytest.fixture
def normal_row():
    """Engine reading with all parameters inside their optimal ranges."""
    return pd.Series({
        "rpm":                 95.0,
        "temperature_exhaust": 350.0,
        "temperature_cooling": 77.0,
        "pressure_lube":       4.2,
        "pressure_fuel":       9.0,
        "vibration_rms":       2.0,
    })


@pytest.fixture
def critical_row():
    """Engine reading with three parameters simultaneously out of range."""
    return pd.Series({
        "rpm":                 95.0,
        "temperature_exhaust": 447.0,   # overheating
        "temperature_cooling": 77.0,
        "pressure_lube":       1.6,     # critically low oil pressure
        "pressure_fuel":       9.0,
        "vibration_rms":       11.5,    # extreme vibration
    })


class TestParameterScore:
    """Tests for the individual parameter scoring function."""

    def test_value_inside_optimal_range_scores_100(self, scorer):
        """A value at the centre of the optimal range must return 100."""
        score = scorer.compute_parameter_score(350.0, 330.0, 370.0)
        assert score == 100.0

    def test_value_at_optimal_boundary_scores_100(self, scorer):
        """Both edges of the optimal range must return exactly 100."""
        assert scorer.compute_parameter_score(330.0, 330.0, 370.0) == 100.0
        assert scorer.compute_parameter_score(370.0, 330.0, 370.0) == 100.0

    def test_value_outside_range_scores_below_100(self, scorer):
        """A value outside the optimal range must score below 100."""
        score = scorer.compute_parameter_score(400.0, 330.0, 370.0)
        assert score < 100.0

    def test_score_never_goes_negative(self, scorer):
        """An extreme outlier must never produce a negative score."""
        score = scorer.compute_parameter_score(900.0, 330.0, 370.0)
        assert score >= 0.0

    def test_score_never_exceeds_100(self, scorer):
        """Score must be capped at 100 regardless of input."""
        score = scorer.compute_parameter_score(350.0, 330.0, 370.0)
        assert score <= 100.0

    @pytest.mark.parametrize("value,expected_min,expected_max", [
        (350.0, 99.0, 100.0),   # centre of range → near 100
        (390.0, 40.0, 80.0),    # moderately elevated
        (450.0,  0.0, 30.0),    # extremely elevated
    ])
    def test_score_degrades_with_deviation(
        self, scorer, value, expected_min, expected_max
    ):
        """Score must decrease monotonically as deviation from optimal grows."""
        score = scorer.compute_parameter_score(value, 330.0, 370.0)
        assert expected_min <= score <= expected_max, \
            f"value={value} → score={score}, expected [{expected_min}, {expected_max}]"


class TestHealthScorer:
    """Tests for the composite health score and DataFrame integration."""

    def test_normal_operation_scores_high(self, scorer, normal_row):
        """All-normal readings must produce a health score of at least 90."""
        score = scorer.compute(normal_row)
        assert score >= 90.0, f"Normal operation should score ≥90, got {score}"

    def test_critical_operation_scores_low(self, scorer, critical_row):
        """Three simultaneous faults must push the health score to 40 or below."""
        score = scorer.compute(critical_row)
        assert score <= 40.0, f"Critical operation should score ≤40, got {score}"

    def test_score_is_between_0_and_100(self, scorer, normal_row):
        """Health score must always be within the [0, 100] range."""
        score = scorer.compute(normal_row)
        assert 0.0 <= score <= 100.0

    def test_add_health_score_adds_columns(self, scorer):
        """add_health_score must add both health_score and health_status columns."""
        df = pd.DataFrame([{
            "rpm": 95.0, "temperature_exhaust": 350.0,
            "temperature_cooling": 77.0, "pressure_lube": 4.2,
            "pressure_fuel": 9.0, "vibration_rms": 2.0,
            "fault_injected": False
        }])
        df_scored = scorer.add_health_score(df)
        assert "health_score"  in df_scored.columns
        assert "health_status" in df_scored.columns

    def test_add_health_score_does_not_modify_original(self, scorer):
        """add_health_score must return a new DataFrame without altering the input."""
        df = pd.DataFrame([{
            "rpm": 95.0, "temperature_exhaust": 350.0,
            "temperature_cooling": 77.0, "pressure_lube": 4.2,
            "pressure_fuel": 9.0, "vibration_rms": 2.0,
            "fault_injected": False
        }])
        original_cols = list(df.columns)
        scorer.add_health_score(df)
        assert list(df.columns) == original_cols

    def test_status_labels_are_correct(self, scorer):
        """Every health_status value must be one of the five valid status labels."""
        df = pd.DataFrame([
            {"rpm": 95.0, "temperature_exhaust": 350.0,
             "temperature_cooling": 77.0, "pressure_lube": 4.2,
             "pressure_fuel": 9.0, "vibration_rms": 2.0,
             "fault_injected": False},
        ])
        df_scored = scorer.add_health_score(df)
        valid_statuses = {"OPTIMAL", "GOOD", "CAUTION", "ALERT", "CRITICAL"}
        for status in df_scored["health_status"]:
            assert str(status) in valid_statuses
