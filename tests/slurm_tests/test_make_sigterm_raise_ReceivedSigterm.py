import time

from cornifer._utilities.multiprocessing import make_sigterm_raise_ReceivedSigterm, ReceivedSigterm

with make_sigterm_raise_ReceivedSigterm():
    time.sleep(1000000)
