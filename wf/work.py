import time
import types
import logging
import threading
import queue
from botocore.config import Config
from langchain_aws import ChatBedrock
from langchain.prompts.chat import (
    ChatPromptTemplate,
    SystemMessagePromptTemplate,
    HumanMessagePromptTemplate,
    AIMessagePromptTemplate,
)
from langchain_core.output_parsers import StrOutputParser
from langgraph.prebuilt import create_react_agent
from wf.prompt_config import PromptsConfig
from wf.state import State

prompts = None
num_threads = 2
state = None
llm = None

log = logging.getLogger("main")
    
def init(prompt_file, num_threads_, checkpoint_file, model_id, temperature, thinking_budget, aws_profile=None):
    global prompts, num_threads, state, llm

    # LLM Init
    bedrock_kwargs = {
        "model_id": model_id,
        "model_kwargs": {
            "max_tokens": 8192,
            "temperature": temperature
        },
        "config": Config(connect_timeout=120, read_timeout=120, retries={"mode": "adaptive"}),
        "streaming": False,
    }
    if thinking_budget:
        bedrock_kwargs["model_kwargs"]["thinking"] = {
            "type": "enabled",
            "budget_tokens": thinking_budget
        }
    if aws_profile:
        bedrock_kwargs["credentials_profile_name"] = aws_profile
    llm = ChatBedrock(**bedrock_kwargs)

    # Others
    prompts = PromptsConfig(prompt_file)
    prompts.fetch()
    num_threads = num_threads_
    state = State(checkpoint_file)

    log.info("Initialization complete")

def get_state(key):
    return state[key]

def execute(step, args=None):
    if (type(step) is str and type(args) is dict and args["prompt"] in state) or (type(step) is str and step in state):
        return
    ret = _execute(step, args)
    if type(step) is str and type(args) is dict:
        # Agent response, 2nd arg is the inference ID
        state[args["prompt"]] = ret
    elif type(step) is str:
        # Regular inference response, step is the inference ID
        state[step] = ret
        
def _execute(step, args=None):
    if isinstance(step, types.FunctionType):
        return step(args)
    elif type(step) is str and type(args) is dict:
        return do_agent(step, args, True)
    elif type(step) is str:
        return do_inference(step)
    raise Exception("Wrong step type")

def _thread_func(input_queue, output_queue):
    log.debug(f"Thread {threading.get_ident()} starting")
    while not input_queue.empty():
        step, args = input_queue.get_nowait()
        try:
            if not type(step) is str or not step in state:
                ret = _execute(step, args)
                output_queue.put((step, ret))
        except Exception as ex:
            log.error(f"Exception in thread {threading.get_ident()} executing step {step}: {ex}")
        finally:
            input_queue.task_done()
    log.debug(f"Thread {threading.get_ident()} exiting")
    
def parallel(steps_arr):
    if all([type(key) is str and key in state for key, _ in steps_arr]):
        return
    
    threads = [None] * num_threads

    # Create input queue
    input_queue = queue.Queue()
    for step, args in steps_arr:
        input_queue.put((step, args))

    # Execute threads
    output_queue = queue.Queue()
    for i in range(min(num_threads, input_queue.qsize())):
        threads[i] = threading.Thread(target=_thread_func, args=(input_queue, output_queue))
        threads[i].start()

    # Wait for them to finish
    input_queue.join()

    # Process output queue
    raw_dict = state.begin_update()
    try:
        while not output_queue.empty():
            step, result = output_queue.get_nowait()
            if type(step) is str:
                raw_dict[step] = result
    finally:
        state.end_update()

    return None

def sequence(steps_arr):
    for step, args in steps_arr:
        execute(step, args)
    return None

def do_inference(inf_id):
    log.info(f"Executing inference for {inf_id}...")
    ret = ""

    # Setup
    sys_msg = SystemMessagePromptTemplate.from_template(prompts.get(f"SYS_{inf_id}", True))
    human_msg = HumanMessagePromptTemplate.from_template(prompts.get(f"HUMAN_{inf_id}"))
    p = ChatPromptTemplate.from_messages([sys_msg, human_msg])
    out_parse = StrOutputParser()
    chain = p | llm | out_parse

    # Inference
    count = 0
    while count < 10:
        try:
            ret = chain.invoke(state.get_dict())
            break
        except llm.client.exceptions.ValidationException:
            log.error(ex)
            raise ex
        except Exception as ex:
            log.warning(f"Retrying... ({str(ex)[:40]}...)")
            time.sleep(5)
        finally:
            count += 1
    return ret

def do_agent(agent_id, inf_args, remove_func_calls):
    log.info(f"Invoking agent {agent_id} with inference {inf_args['prompt']}")
    ret = ""

    # Setup
    sys_msg = SystemMessagePromptTemplate.from_template(prompts.get(f"SYSA_{agent_id}", True))
    sys_msg = sys_msg.format(**state.get_dict())
    human_msg = HumanMessagePromptTemplate.from_template(prompts.get(f"HUMANA_{inf_args['prompt']}"))
    human_msg = human_msg.format(**state.get_dict())

    # Create agent
    agent = create_react_agent(llm, inf_args["tools"], prompt=sys_msg)

    # Invoke agent
    max_iterations = 10
    recursion_limit = 2 * max_iterations + 1
    resp = agent.invoke(
        {"messages": [("human", human_msg.content)]},
        {"recursion_limit": recursion_limit}
    )
    messages = resp["messages"]
    for msg in messages:
        log.debug(f"  {msg.type}: '{msg.content}'")
    ret = messages[-1].content
    if remove_func_calls:
        ret = purge_function_calls_from_output(ret)
    return ret
    
def purge_function_calls_from_output(output_str):
    edited = output_str
    while True:
        prev_version = edited
        edited = snip_from_text("<function_calls>", "</function_calls>", prev_version)
        if prev_version == edited:
            break
    return edited

def snip_from_text(start, end, text, inclusive=True):
    if not text or len(text) == 0:
        return text
    start_idx = -1
    if type(start) is str:
        start_idx = text.find(start)
        if not inclusive and start_idx != -1:
            start_idx += len(start)
    end_idx = -1
    if type(end) is str:
        end_idx = text.find(end)
        if inclusive and end_idx != -1:
            end_idx += len(end)
    # print(f"{start_idx}, {end_idx}")
    if start_idx == end_idx:
        return text
    elif start_idx == -1:
        return text[end_idx:]
    elif end_idx == -1:
        return text[:start_idx]
    return text[:start_idx] + text[end_idx:]
