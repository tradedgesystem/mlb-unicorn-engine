from datetime import date

from backend.app.core.roles import get_pitcher_usage_counts


def test_get_pitcher_usage_counts_skips_none_pitcher_ids():
    class DummyRow:
        def __init__(self, pitcher_id=None, starts=0, apps=0):
            self.pitcher_id = pitcher_id
            self.starts = starts
            self.apps = apps

    class DummySession:
        def execute(self, _):
            return [DummyRow(None, starts=1, apps=2), DummyRow(123, starts=2, apps=3)]

        def scalar(self, _):
            return None

    session = DummySession()
    counts = get_pitcher_usage_counts(session, as_of_date=date.today(), lookback_days=30)
    # None pitcher_id rows should be skipped, valid rows retained.
    assert 123 in counts
    assert counts[123]["starts"] == 2
    assert counts[123]["apps"] == 3
