import builtins

from soda_yaml.soda_yaml import evaluate_yaml_expression
from soda_yaml.soda_yaml import from_yaml
from soda_yaml.soda_yaml import load_yaml
from soda_yaml.soda_yaml import dump_yaml
from soda_yaml.soda_yaml import YamlEvaluationError

import subprocess

from concurrent import futures
from concurrent.futures import ThreadPoolExecutor

import logging

import os
import sys


def call_process(cmd_list):
    """
    Execute a command line from a list of arguments
    """
    return_code = 0
    output = ""
    try:
        output = subprocess.check_output(cmd_list, stderr=subprocess.STDOUT)
        output = output.decode("utf-8")
    except PermissionError as permission_error:
        logging.error("Permission denied to launch '%s':\n%s", cmd_list,
                      permission_error)
        return_code = -1
    except subprocess.CalledProcessError as err:
        error = err.output.decode("utf-8")
        logging.error("Fail to launch command: '%s'\n%s", " ".join(cmd_list),
                      error)
        return_code = -1
    except FileNotFoundError:
        # This exception is raised if the first arg of cmd_list is not a valid
        # comand.
        logging.error("Command not found: '%s'.", cmd_list[0])
        return_code = -1
    except TypeError as type_error:
        logging.warning("Fail to format process output:\n%s", type_error)

    if(return_code == 0):
        logging.info(output)
    return return_code


def process_in_scope(expr, pipe_step_doc):
    """
    Look if the process have already been launched with success,
    if it's not the case it get the command line given within
    the pipe_step_doc dictionnary, evaluate it  and launch it
    """
    pipe_step_name = from_yaml(pipe_step_doc, '__NAME__')
    state_filename = os.path.join(builtins.SODA_STATE_DIR,
                                  pipe_step_name + ".yaml")
    if(os.path.exists(state_filename)):
        states = load_yaml(state_filename)
        if expr in states.keys():
            if not states[expr]:
                return 0

    cmd_list = from_yaml(pipe_step_doc, '__CMD__')
    try:
        cmd_list = [evaluate_yaml_expression(arg, scope_expr=expr)
                    for arg in cmd_list]
    except YamlEvaluationError as err:
        logging.error("Unable to evaluate yaml variable:\n%s", str(err))
        return -1

    return_code = call_process(cmd_list)
    return return_code


def submit_process(exprs, pipe_step_doc):
    """
    Create a pool of thread depending on the maximum
    umber of workers (SODA_MAXWORKERS) when a worker finish its job,
    print the actual progression of this step of the pipeline
    ans write the retuen code of the command line in a yaml doc
    (located at SODA_STATE_DIR).
    """
    descrition = from_yaml(pipe_step_doc, '__DESCRIPTION__')
    print("{0}: {1}%\033[K\r".format(descrition, 0), end="")
    sys.stdout.flush()

    with ThreadPoolExecutor(max_workers=builtins.SODA_MAXWORKERS) as executor:
        expr_for_future = dict()
        for expr in exprs:
            future = executor.submit(process_in_scope,
                                     expr,
                                     pipe_step_doc)
            expr_for_future[future] = expr

        progression = 1
        pipe_step_name = from_yaml(pipe_step_doc, '__NAME__')
        state_filename = os.path.join(builtins.SODA_STATE_DIR,
                                      pipe_step_name + ".yaml")
        states = dict()
        for future in futures.as_completed(expr_for_future.keys()):
            # Get the return code of the cmd and store it.
            expr = expr_for_future[future]
            # try:
            states[expr] = future.result()

            if (states[expr] == 0):
                with builtins.SODA_LOCK:
                    print("{0}: {1:.0%}\033[K\r".format(descrition,
                          progression / len(exprs)), end="")
                    # sys.stdout.flush()
                progression += 1

            # Dump the retrun codes in a yaml doc
            dump_yaml(states, state_filename)

        # Rewrite an empty line
        print("")
