import random

BASE_DELAY = 30
MAX_DELAY = 30 * 60


def retry_delay(
    retry_number: int,
    base_delay: float = BASE_DELAY,
    max_delay: float = MAX_DELAY,
) -> float:
    """
    Wait for a base delay before the first retry and then double down on each
    additional retry up to a maximum delay. In seconds.

    An API that has issues is usually down for quite some time. So increasing
    waiting times between retries avoid to hammer it
    """
    delay = min(base_delay * 2**retry_number, max_delay)
    return delay * random.uniform(0.5, 1.0)
