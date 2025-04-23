import logging

import redis


def flush_cache(host, port, db, password):
    redis_client = redis.Redis(
        host=host,
        port=port,
        db=db,
        password=password,
    )
    # Delete keys in the background in a different thread without blocking the server
    flush_command = redis_client.execute_command("FLUSHALL ASYNC")
    logging.info(f"Flush cache command status: {flush_command}")
    if redis_client.keys():
        raise Exception(f"****** Could not flush cache: {redis_client.keys()}")
