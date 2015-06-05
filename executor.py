from log import quit_with_error
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor
from evaluator import Evaluator
from yaml_io import YamlIO
try:
    import path
except ImportError:
    quit_with_error("Pesto requiered path.py to be installed, "
                    "checkout requirement.txt.")
import sys
import subprocess
import logging


class PipelineExecutor():
    _pipeline = None
    _print_only = False
    _force_execution = False

    OKGREEN = '\033[92m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    RETURN = '\033[K\r'

    @property
    def print_only(self):
        return self._print_only

    @print_only.setter
    def print_only(self, value):
        self._print_only = value

    @property
    def force_execution(self):
        return self._print_only

    @force_execution.setter
    def force_execution(self, value):
        self._force_execution = value

    def _print_progression(self, desc, prog, is_ok):
        if is_ok:
            color = self.OKGREEN
        else:
            color = self.FAIL

        print("{0}{1}: {2:.0%}{3}".format(color,
                                       desc,
                                       prog,
                                       self.ENDC + self.RETURN), end="")
        sys.stdout.flush()

    def execute(self, node_name=None):
        if node_name is None:
            node = self._pipeline.root
        else:
            try:
                node = self._pipeline.nodes[node_name]
            except KeyError:
                quit_with_error("Unable to find node '\033[91m{}\033[0m\033[1m'"
                                " in pipeline".format(node_name))
            if self._print_only:
                self._print_one_node(node)
            else:
                self._execute_one_node(node)
        for n in self._pipeline.walk(node):
            if self._print_only:
                self._print_one_node(n)
            else:
                self._execute_one_node(n)

    def _execute_one_node(self, node):
        pass

    def _print_one_node(self, node):
        if self._print_only:
            ENDC = '\033[0m'
            BOLD = '\033[1m'
            print(BOLD, "\nExecuting: ", node.name, ENDC)
            for scope_value in node.scope.values:
                evaluator = Evaluator(scope_value)
                cmd_str = evaluator.evaluate(" ".join(node.cmd))
                print(cmd_str)

    def _execute_one_scope_value(self, node, scope_value, results):
        if not self._force_execution and scope_value in results:
            if not results[scope_value]:
                return 0

        evaluator = Evaluator(scope_value)
        cmd = [evaluator.evaluate(arg) for arg in node.cmd]

        return_code = 0
        output = ""
        try:
            output = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
            output = output.decode("utf-8")
        except PermissionError as permission_error:
            logging.error("Permission denied to launch '%s':\n%s", " ".join(cmd),
                          permission_error)
            return_code = -1
        except subprocess.CalledProcessError as err:
            error = err.output.decode("utf-8")
            logging.error("Fail to launch command: '%s'\n%s", " ".join(cmd),
                          error)
            return_code = -1
        except FileNotFoundError:
            # This exception is raised if the first arg of cmd is not a valid
            # comand.
            logging.error("Command not found: '%s'.", cmd[0])
            return_code = -1
        except TypeError as type_error:
            logging.warning("Fail to format process output:\n%s", type_error)

        if(return_code == 0):
            logging.info(output)
        return return_code


class ThreadedPipelineExecutor(PipelineExecutor):
    _max_workers = 0
    _scope_values = None

    _LOCK = futures.thread.threading.Lock()

    def __init__(self, pipeline, max_workers):
        self._max_workers = max_workers
        self._pipeline = pipeline
        self._scope_values = dict()

    def _execute_one_node(self, node):
        self._futures = dict()
        max_workers = self._max_workers * node.workers_modifier

        node_filname = path.Path(".pesto" + node.name + ".yaml")
        if node_filname.exists():
            results = YamlIO.load_yaml(node_filname)
        else:
            results = dict()

        with ThreadPoolExecutor(max_workers) as ex:
            for scope_value in node.scope.values:
                fut = ex.submit(self._execute_one_scope_value,
                                node,
                                scope_value,
                                results)
                self._scope_values[fut] = scope_value

            progression = 0
            is_ok = True
            for future in futures.as_completed(self._scope_values.keys()):
                scope_value = self._scope_values[future]
                result = future.result()
                results[scope_value] = result
                if result == 0:
                    progression += 1
                else:
                    is_ok = False
                # show progression
                with self._LOCK:
                    self._print_progression(node.description, 
                                            progression / len(node.scope.values), 
                                            is_ok)
                # dump results.
                YamlIO.dump_yaml(results, node_filname)
        # print new line
        print("")
