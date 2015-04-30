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

import signal


OKGREEN = '\033[92m'
FAIL = '\033[91m'
ENDC = '\033[0m'
RETURN = '\033[K\r'


class DelayedKeyboardInterrupt(object):
    def __enter__(self):
        self.signal_received = False
        self.old_handler = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, self.handler)

    def handler(self, signal, frame):
        self.signal_received = (signal, frame)
        logging.info('SIGINT received. Delaying KeyboardInterrupt.')

    def __exit__(self, type, value, traceback):
        signal.signal(signal.SIGINT, self.old_handler)
        if self.signal_received:
            self.old_handler(*self.signal_received)

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


def process_in_scope(current_expr, pipe_step_doc, result_for_expr):
    """
    Look if the process have already been launched with success,

    If it's not the case, it get the command line given within
    the 'pipe_step_doc' dictionnary, evaluate it  and launch it
    """
    if current_expr in result_for_expr.keys():
        if not result_for_expr[current_expr]:
            return 0

    cmd_list = from_yaml(pipe_step_doc, '__CMD__')
    try:
        cmd_list = [evaluate_yaml_expression(arg, current_expr=current_expr)
                    for arg in cmd_list]
    except YamlEvaluationError as err:
        logging.error("Unable to evaluate yaml variable:\n%s", str(err))
        return -1
    # we had a non string arg, something wrong happen so we return -1.
    except TypeError as err:
        logging.error("Error in commmand: %s\n%s", cmd_list, err)
        return -1

    return_code = call_process(cmd_list)
    return return_code


def submit_process(exprs, pipe_step_doc):
    """
    Create a pool of thread depending on the maximum
    number of workers ('SODA_MAXWORKERS').

    When a worker finish its job,
    print the progression of the actual step of the pipeline
    ansd write the return code of the command line in a yaml doc
    (located at 'SODA_STATE_DIR').
    """
    descrition = from_yaml(pipe_step_doc, '__DESCRIPTION__')
    print("{0}: {1}%{2}".format(descrition, 0, RETURN), end="")
    sys.stdout.flush()

    result_for_expr = dict()
    pipe_step_name = from_yaml(pipe_step_doc, '__NAME__')
    result_for_expr_filename = os.path.join(builtins.SODA_STATE_DIR,
                                            pipe_step_name + ".yaml")
    if(os.path.exists(result_for_expr_filename)):
        result_for_expr = load_yaml(result_for_expr_filename)

    with ThreadPoolExecutor(max_workers=builtins.SODA_MAXWORKERS) as executor:
        expr_for_future = dict()
        for current_expr in exprs:
            future = executor.submit(process_in_scope,
                                     current_expr,
                                     pipe_step_doc,
                                     result_for_expr)
            expr_for_future[future] = current_expr

        progression = 0
        col = OKGREEN
        for future in futures.as_completed(expr_for_future.keys()):
            # Get the return code of the cmd and store it.
            expr = expr_for_future[future]

            res = future.result()
            result_for_expr[expr] = res

            if (res == 0):
                progression += 1
            else:
                col = FAIL
            with builtins.SODA_LOCK:
                    print("{0}{1}: {2:.0%}{3}{4}".format(
                          col,
                          descrition,
                          progression / len(exprs),
                          ENDC,
                          RETURN
                          ), end="")

            # Dump the retrun codes in a yaml doc
            with DelayedKeyboardInterrupt:
                dump_yaml(result_for_expr, result_for_expr_filename)

        # Rewrite an empty line
        print("")
