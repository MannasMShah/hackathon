import time

def with_retry(fn, retries=5, backoff=0.5):
    for i in range(retries):
        try:
            return fn()
        except Exception as e:
            if i == retries - 1:
                raise
            time.sleep(backoff * (2 ** i))
