import sys
import argparse
import logging
import random
import importlib
from pathlib import Path

def rand_str(length):
    ret = ""
    for i in range(length):
        r = random.randrange(26)
        ret += chr(r + 65)
    return ret

parser = argparse.ArgumentParser()
parser.add_argument("-w", "--workflow", required=True, help="Workflow to run")
parser.add_argument("-n", "--num-threads", required=False, type=int, default=2, help="Number of concurrent threads to run")
parser.add_argument("-a", "--aws-profile", required=False, help="AWS profile to use")
parser.add_argument("-c", "--checkpoint", required=False, help="Checkpoint file to use, file-name only (only specify when continuing from a previously interrupted workflow)")
parser.add_argument("-t", "--temperature", type=float, required=False, default=.7, help="The LLM temperature to use")
parser.add_argument("-b", "--thinking-budget", type=int, required=False, default=0, help="The LLM thinking budget")
parser.add_argument("-m", "--model-id", required=False, default="anthropic.claude-3-5-haiku-20241022-v1:0", help="Bedrock model id to use")

args = parser.parse_args()

import wf.work

formatter = logging.Formatter(fmt="%(asctime)s %(thread)d %(levelname)s: %(message)s", datefmt="%H:%M:%S")
handler = logging.StreamHandler(stream=sys.stdout)
def setup_log(log, level):
    log.setLevel(level)
    log.propagate = False
    handler.setLevel(level)
    handler.setFormatter(formatter)
    log.addHandler(handler)
    
setup_log(logging.getLogger(), logging.INFO)
log = logging.getLogger("main")
setup_log(log, logging.DEBUG)

def main():
    # Init vars
    cur_dir = Path(__file__).parent
    wf_dir = cur_dir / "definitions" / args.workflow
    if not wf_dir.exists():
        print("Specified workflow doesn't exist")
        return
    prompt_file = wf_dir / "prompts.txt"
    cp_dir = wf_dir / "checkpoints"
    if args.checkpoint and ("/" in args.checkpoint or "\\" in args.checkpoint):
        print("Checkpoint argument should not contain any folders; only specify the file name")
        return
    if not cp_dir.exists():
        cp_dir.mkdir()
    checkpoint_file = cp_dir / (args.checkpoint or f"checkpoint-{rand_str(6)}.json")
    num_threads = args.num_threads
    model_id = args.model_id
    aws_profile = args.aws_profile

    # Init function call
    wf.work.init(prompt_file, num_threads, checkpoint_file, model_id, args.temperature, args.thinking_budget, aws_profile)

    # Import workflow module
    module = importlib.import_module(f"definitions.{args.workflow}.main")

    try:
        log.info("Executing workflow")
        module.do_wf()
        log.info("Done!")
    except Exception as ex:
        log.exception(ex)
        # log.error(f"Exception executing workflow: {ex}\n\nCheckpoint file: {checkpoint_file}")

if __name__ == "__main__":
    main()
    
