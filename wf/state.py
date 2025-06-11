import json
import threading
import logging

log = logging.getLogger("main")

class State:
    def __init__(self, checkpoint_file):
        self.lock = threading.Lock()
        self.file_ = checkpoint_file

        log.info(f"Attempting to read checkpoint file {self.file_}...")
        try:
            with open(self.file_, "r") as f:
                self.dict_ = json.load(f)
                log.info("Checkpoint loaded successfully")
        except:
            self.dict_ = {}

    def get_dict(self):
        return self.dict_

    def begin_update(self):
        self.lock.acquire()
        return self.dict_

    def end_update(self):
        try:
            with open(self.file_, "w") as f:
                json.dump(self.dict_, f)
            self.lock.release()
        except Exception as ex:
            log.error(f"Error writing state to checkpoint file: {ex}")
            raise ex
        
    def __contains__(self, key):
        return key in self.dict_
    
    def __getitem__(self ,key):
        return self.dict_[key]

    def __setitem__(self, key, val):
        try:
            with self.lock:
                self.dict_[key] = val
                with open(self.file_, "w") as f:
                    json.dump(self.dict_, f)
        except Exception as ex:
            log.error(f"Error writing state to checkpoint file: {ex}")
            raise ex
