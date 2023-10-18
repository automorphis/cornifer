import time

def start_with_timeout(procs, timeout, query_wait = 0.1):

    for proc in procs:
        proc.start()

    start = time.time()

    while time.time() - start <= timeout:

        if all(not proc.is_alive() for proc in procs):
            return

        time.sleep(query_wait)

    print("terminated")

    for p in procs:
        p.terminate()
