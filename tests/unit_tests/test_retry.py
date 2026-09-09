from unittest.mock import patch

from data_pipelines_annuaire.helpers.retry import retry_delay


class TestRetryDelay:
    def test_waiting_doubles_at_every_retry(self):
        with patch("random.uniform", return_value=1.0):
            assert [retry_delay(n) for n in range(4)] == [30, 60, 120, 240]

    def test_waiting_is_capped_at_thirty_minutes(self):
        with patch("random.uniform", return_value=1.0):
            assert retry_delay(20) == 30 * 60

    def test_waiting_is_jittered(self):
        with patch("random.uniform", return_value=0.5):
            assert retry_delay(0) == 15

    def test_caller_keeps_its_own_pace(self):
        with patch("random.uniform", return_value=1.0):
            delays = [retry_delay(n, base_delay=0.3, max_delay=10) for n in range(6)]

        assert delays == [0.3, 0.6, 1.2, 2.4, 4.8, 9.6]

    def test_caller_pace_is_capped_too(self):
        with patch("random.uniform", return_value=1.0):
            assert retry_delay(10, base_delay=0.3, max_delay=10) == 10
