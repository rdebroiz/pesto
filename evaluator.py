import logging
import re

from pprint import pformat


class Evaluator():
    _helpers = None
    _cur_scope_value = None

    @classmethod
    def set_helpers(cls, helpers):
        cls._helpers = helpers

    def __init__(self, cur_scope_value=None):
        if (self._helpers is None):
            logging.error("Evaluator created without any _helpers.\n")
        self._cur_scope_value = cur_scope_value
        if(self._cur_scope_value is None):
            self._cur_scope_value = ""

    def evaluate(self, string):
        """
        Try to evaluate staticly or dynamicly the given yaml string
        while a static ${key} or a dynamic ?{key} has been found inside.

        'current_expr' is needed by 'evaluate_dynamic_expression()'
        to know in which files inside which scope seeking for a match
        with the regular expression associated to the dynamic ?{key}
        """
        all_evaluated = False
        while(not all_evaluated):
            try:
                match_dolls = re.search(r"\$\{(.*?)\}", string)
                match_quest = re.search(r"\?\{(.*?)\}", string)
            except TypeError:
                logging.error("expression to evaluate is not of type String: "
                              "'\033[91m%s\033[0m\033[1m'", string)
                raise
            try:
                if(match_dolls):
                    string = self._evaluate_static(string,
                                                   match_dolls.group(1))
                if(match_quest):
                    string = self._evaluate_dynamic(string,
                                                    match_quest.group(1))
            except KeyError:
                logging.error("unable to evaluate expression: '%s'",
                              string)
                raise
            if(not match_dolls and not match_quest):
                all_evaluated = True
        return string

    def _evaluate_static(self, string, to_evaluate):
        """
        Subsitute the ${to_evaluate} key by the associated value
        found in DataModel in 'string'.
        """
        return re.sub(r"\$\{" + to_evaluate + r"}",
                      self._get_value_from_helpers(to_evaluate),
                      string)

    def _evaluate_dynamic(self, string, to_evaluate):
        """
        First parse the ?{to_evaluate} key,
        if there is a match for '->' 'to_evaluate' is replaced
        by what is precedding '->'.
        What is following '->' is considered as a key entry in
        'DataModel', 'current_expr' is replaced by the value
        corresponding to that key.

        Then get of all files from 'DataModel.files' having a path matching
        the regular expression 'current_expr'

        'to_evaluate' is then considered as a key entry in
        'DataModel'. Later we seek for a match between
        the regular expression  given by that key and the list of
        filenames computed before. An error is raised if more than one match
        has been found.

        Finally substitute the ?{to_evaluate} by what have been found
        in 'yaml_string'
        """
        from data_model import DataModel
        match_redirect = re.search(r"(.*?)->(.*)", to_evaluate)
        to_substitute = to_evaluate
        scope_value = self._cur_scope_value
        if match_redirect:
            to_evaluate = match_redirect.group(1)
            scope_name = match_redirect.group(2)
            scope = DataModel.scopes[scope_name]
            scope_value = self.evaluate(scope.expression)
        files_matching_scope_value = [f for f in DataModel.files
                                      if re.search(scope_value, f)]
        evaluated_value = set()
        evltr = Evaluator(scope_value)
        reg_exp = evltr.evaluate(self._get_value_from_helpers(to_evaluate))

        for f in files_matching_scope_value:
            match_eval = re.search(reg_exp, f)
            if match_eval:
                evaluated_value.add(match_eval.group(0))
        if (len(evaluated_value) != 1):
            msg = ("Bad evaluation of '{0}', within: '{1}'\n"
                   "Matches are:{2}".format(to_substitute,
                                            string,
                                            pformat(evaluated_value)))
            logging.error(msg)
            raise KeyError("")
        new = evaluated_value.pop()
        string = re.sub(r"\?\{" + to_substitute + r"\}", new, string)
        return string

    def _get_value_from_helpers(self, key):
        try:
            return self._helpers[key]
        except KeyError:
            RED = '\033[91m'
            ENDC = '\033[0m'
            logging.error("unable to find any key '\033[91m%s\033[0m\033[1m' "
                          "in configuration file",
                          key)
