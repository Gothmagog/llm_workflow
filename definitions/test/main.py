import re
import wf.work as w
import logging

log = logging.getLogger("test_wf")
log.setLevel(logging.DEBUG)

def do_wf():
    w.execute("datacommons", "test")
    print(w.state["test"])
