import builtins

import logging
import sys
import re
from pprint import pformat

try:
    import yaml
except ImportError as err:
    print("Soda need pyyaml", err)
    sys.exit(1)


class YamlEvaluationError(LookupError):
    """
    An error raised when the evaluation of ${statitc_key} or
    ?{dynamic_key} faile
    """
    def __init__(self, arg):
        super(YamlEvaluationError, self).__init__()
        self.arg = arg


class YamlLookUpError(LookupError):
    """
    An error raised when a
    key is not found within a yaml dicionnary
    """
    def __init__(self, arg):
        super(YamlLookUpError, self).__init__()
        self.arg = arg


def load_yaml(yaml_filename):
    """
    Load a single yaml document from a file in a thread safe context.
    """
    try:
        with builtins.SODA_LOCK:
            yaml_doc = yaml.load(open(yaml_filename, 'r'))
    except OSError as oserr:
        logging.error("Loading %s: %s", yaml_filename, oserr)
        sys.exit(1)
    except (yaml.YAMLError, yaml.scanner.ScannerError) as yamlerr:
        logging.error("Loading %s: %s:", yaml_filename, yamlerr)
        sys.exit(1)
    return yaml_doc


def load_all_yaml(yaml_filename):
    """
    Load a list of yaml documents from a file in a thread safe context.
    """
    try:
        with builtins.SODA_LOCK:
            yaml_docs = yaml.load_all(open(yaml_filename, 'r'))
        # stream.close()
    except OSError as oserr:
        logging.error("Loading %s: %s", yaml_filename, oserr)
        sys.exit(1)
    except (yaml.YAMLError) as yamlerr:
        logging.error("Loading %s: %s:", yaml_filename, yamlerr)
        sys.exit(1)
    return list(yaml_docs)


def dump_yaml(to_dump, yaml_filename):
    """
    Dump a python object inside a yaml document in a thread safe context
    """
    try:
        with builtins.SODA_LOCK:
            yaml.dump(to_dump, open(yaml_filename, 'w'),
                      default_flow_style=False)
    except (yaml.error.YAMLError) as yamlerr:
        logging.error("Dumping %s in %s: %s:",
                      yaml_filename,
                      to_dump,
                      yamlerr)


def escape_reserved_re_char(string):
    """
    Escape with a backslash characters reserved by regular expressions
    in the given string.
    """
    return re.sub(r"(?P<char>[()*.?^\[\]\\${}+|])", r"\\\g<char>", string)


def evaluate_dynamic_expression(yaml_string, to_evaluate, current_expr):
    """
    First parse the ?{to_evaluate} key,
    if there is a match for '->' 'to_evaluate' is replaced
    by what is precedding '->'.
    What is following '->' is considered as a key entry in
    'SODA_DATA_STRUCTURE', 'current_expr' is replaced by the value
    corresponding to that key.

    Then get of all files from 'SODA_FILES_IN_ROOT' having a path matching
    the regular expression 'current_expr'

    'to_evaluate' is then considered as a key entry in
    'SODA_DATA_STRUCTURE'. Later we seek for a match between
    the regular expression  given by that key and the list of
    filenames computed before. An error is raised if more than one match
    has been found.

    Finally substitute the ?{to_evaluate} by what have been found
    in 'yaml_string'
    """
    match_redirect = re.search(r"(.*?)->(.*)", to_evaluate)
    to_substitute = to_evaluate
    if match_redirect:
        to_evaluate = match_redirect.group(1)
        scope = match_redirect.group(2)
        pattern = from_yaml(builtins.SODA_DATA_STRUCTURE, scope,
                            current_expr=current_expr)
        current_expr = re.search(pattern, current_expr).group(0)

    files_in_scope = [f for f in builtins.SODA_FILES_IN_ROOT
                      if re.search(current_expr, f)]

    evaluated_value = set()
    expression = from_yaml(builtins.SODA_DATA_STRUCTURE,
                           to_evaluate,
                           current_expr=current_expr)

    for f in files_in_scope:
        match_eval = re.search(expression, f)
        if match_eval:
            evaluated_value.add(match_eval.group(0))
    if (len(evaluated_value) != 1):
        msg = "Bad evaluation of '{0}', within '{1}'\n"\
              "Matches are:{2}".format(to_substitute,
                                       yaml_string,
                                       pformat(evaluated_value))
        logging.error(msg)
        return ""

    new = evaluated_value.pop()
    yaml_string = re.sub(r"\?\{" + to_substitute + r"\}", new, yaml_string)
    return yaml_string


def evaluate_static_expression(yaml_string, to_evaluate, current_expr=''):
    """
    Subsitute the ${to_evaluate} key by the associated value
    found in SODA_DATA_STRUCTURE in 'yaml_string'.
    """
    yaml_string = re.sub(r"\$\{" + to_evaluate + r"\}",
                         from_yaml(builtins.SODA_DATA_STRUCTURE, to_evaluate,
                                   current_expr=current_expr),
                         yaml_string)
    return yaml_string


def evaluate_yaml_expression(yaml_string, current_expr=''):
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
            match_dolls = re.search(r"\$\{(.*?)\}", yaml_string)
            match_quest = re.search(r"\?\{(.*?)\}", yaml_string)
        except TypeError:
            logging.error("Attempt to evaluate non string expression "
                          "from yaml document: %s", yaml_string)
            raise

        if(match_dolls):
            yaml_string = evaluate_static_expression(yaml_string,
                                                     match_dolls.group(1),
                                                     current_expr)
        if(match_quest):
            yaml_string = evaluate_dynamic_expression(yaml_string,
                                                      match_quest.group(1),
                                                      current_expr)
        if(not match_dolls and not match_quest):
            all_evaluated = True

    return yaml_string


def from_yaml(yaml_dic, key, current_expr=''):
    """
    Return the value (evaluated) corresponding to the key 'key'
    in the yaml_dic dictionary
    """
    try:
        value = yaml_dic[key]
    except KeyError:
        logging.error("YAML error: key '%s' not found.", key)
        return ""

    if isinstance(value, str):
        value = evaluate_yaml_expression(value, current_expr)

    return value
