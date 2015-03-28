import builtins

from soda_yaml.soda_yaml import evaluate_yaml_expression
from soda_yaml.soda_yaml import from_yaml
from soda_yaml.soda_yaml import load_yaml
from soda_yaml.soda_yaml import dump_yaml

import subprocess

from concurrent import futures
from concurrent.futures import ThreadPoolExecutor

import logging
from pprint import pformat

import re
import os
import sys


def call_process(cmd_list):
    return_code = 0
    output = ""
    try:
        output = subprocess.check_output(cmd_list, stderr=subprocess.STDOUT)
        output = output.decode("utf-8")
    except PermissionError:
        logging.critical("Permission denied to launch: %s", cmd_list)
        return 1
    except subprocess.CalledProcessError as err:
        try:
            error = err.output.decode("utf-8")
            logging.critical("return code:", err.returncode, "\n",
                             error)
        except TypeError as err2:
            logging.warning("fail to format process output:\n%s", err2)
        finally:
            raise Exception("Error during  call_process")

    logging.info(output)
    return return_code


def process_in_scope(pipe_step_doc, to_process):
    series = [s for s in builtins.SODA_SERIES if re.search(to_process, s)]

    pipe_step_name = from_yaml(pipe_step_doc, '_NAME')
    state_filename = os.path.join(builtins.SODA_STATE_DIR,
                                  pipe_step_name + ".yaml")
    if(os.path.exists(state_filename)):
        states = load_yaml(state_filename)
        if to_process in states.keys():
            if not states[to_process]:
                return 0

    cmd_list = from_yaml(pipe_step_doc, '_CMD')
    cmd_list = [evaluate_yaml_expression(arg, cur_series=series)
                for arg in cmd_list]
    logging.debug(pformat(cmd_list))

    try:
        return_code = call_process(cmd_list)
    except Exception:
        logging.critical("Error happend during process of %s, for %s:",
                         pipe_step_name, to_process)
        return_code = 1
    finally:
        return return_code


def submit_process(scoped_expr_list, pipe_step_doc):
    descrition = from_yaml(pipe_step_doc, '_DESCRIPTION')
    print("{0}: {1}%".format(descrition, 0), end="")
    sys.stdout.flush()

    with ThreadPoolExecutor(max_workers=builtins.SODA_MAXWORKERS) as executor:
        scoped_expr_for_future = dict()
        for scoped_expr in scoped_expr_list:
            future = executor.submit(process_in_scope,
                                     pipe_step_doc,
                                     scoped_expr)
            scoped_expr_for_future[future] = scoped_expr

        progression = 1
        pipe_step_name = from_yaml(pipe_step_doc, '_NAME')
        state_filename = os.path.join(builtins.SODA_STATE_DIR,
                                      pipe_step_name + ".yaml")
        states = dict()
        for future in futures.as_completed(scoped_expr_for_future.keys()):
            # Get the return code of the cmd and store it.
            scoped_expr = scoped_expr_for_future[future]
            # try:
            states[scoped_expr] = future.result()

            if (states[scoped_expr] == 0):
                with builtins.SODA_LOCK:
                    print("\033[K\r{0}: {1:.0%}".format(descrition,
                          progression / len(scoped_expr_list)), end="")
                    sys.stdout.flush()
                progression += 1

            # Dump the retrun codes in a yaml doc
            dump_yaml(states, state_filename)

        # Rewrite an empty line
        print("\033[K\r")
