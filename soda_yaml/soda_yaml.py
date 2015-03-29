import builtins

import logging
import sys
import re
from pprint import pformat

try:
    import yaml
except ImportError as err:
    print("Soda need pyyaml", err)


class YamlEvaluationError(LookupError):
    """An error raised when the evaluation of ${statitc_key} or
    ?{dynamic_key} faile"""
    def __init__(self, arg):
        super(YamlEvaluationError, self).__init__()
        self.arg = arg


class YamlLookUpError(LookupError):
    """An error raised when a
    key is not found within a yaml dicionnary"""
    def __init__(self, arg):
        super(YamlLookUpError, self).__init__()
        self.arg = arg


def load_yaml(yaml_filename):
    """Load a single yaml document from a file in a thread safe context."""
    try:
        with builtins.SODA_LOCK:
            yaml_doc = yaml.load(open(yaml_filename, 'r'))
    except OSError as oserr:
        logging.error("Loading %s: %s", yaml_filename, oserr)
        sys.exit(1)
    except (yaml.error.YAMLError) as yamlerr:
        logging.error("Loading %s: %s:", yaml_filename, yamlerr)
        sys.exit(1)
    return yaml_doc


def load_all_yaml(yaml_filename):
    """Load a list of yaml documents from a file in a thread safe context."""
    try:
        with builtins.SODA_LOCK:
            yaml_docs = yaml.load_all(open(yaml_filename, 'r'))
        # stream.close()
    except OSError as oserr:
        logging.error("Loading %s: %s", yaml_filename, oserr)
        sys.exit(1)
    except (yaml.error.YAMLError) as yamlerr:
        logging.error("Loading %s: %s:", yaml_filename, yamlerr)
        sys.exit(1)
    return list(yaml_docs)


def dump_yaml(to_dump, yaml_filename):
    """Dump a python object inside yaml document in a thread safe context"""
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
    """Escape wit a back slash all char reserved for regular expression
    in the given string."""
    return re.sub(r"(?P<char>[()*.?^\[\]\\${}+|])", r"\\\g<char>", string)


def evaluate_dynamic_expression(value, match, scope_expr):
    to_evaluate = match.group(1)
    match_redirect = re.search(r"(.*?)->(.*)", to_evaluate)
    if match_redirect:
        to_evaluate = match_redirect.group(1)
        scope = match_redirect.group(2)
        scope_expr = from_yaml(builtins.SODA_DATA_STRUCTURE, scope)

    files_in_scope = [f for f in builtins.SODA_FILES_IN_ROOT
                      if re.search(scope_expr, f)]

    evaluated_value = set()
    expression = from_yaml(builtins.SODA_DATA_STRUCTURE,
                           to_evaluate)

    for f in files_in_scope:
        match_eval = re.search(expression, f)
        if match_eval:
            evaluated_value.add(match_eval.group(0))
    if (len(evaluated_value) != 1):
        msg = "Bad evaluation of '{0}', within '{1}'\n"\
              "Matches are:{2}".format(match.group(1),
                                       value,
                                       pformat(evaluated_value))
        logging.error(msg)
        return ""

    new = evaluated_value.pop()
    value = re.sub(r"\?\{" + match.group(1) + r"\}", new, value)
    return value


def evaluate_static_expression(value, match):
    value = re.sub(r"\$\{" + match.group(1) + r"\}",
                   from_yaml(builtins.SODA_DATA_STRUCTURE,
                             match.group(1)),
                   value)
    return value


def evaluate_yaml_expression(value, scope_expr=''):
    """Evaluate an expression of a yaml document.
    An expression can be static: ${key} or dynamic: ?{key}
    If the expression is static it will just be replace
    by the value corresponding to the given key in the dictionnary
    SODA_DATA_STRUCTURE (the first document in the yaml config files).
    If the expression is dynamic it will be replace by the string
    matching the regular expression give by the value corresponding
    to the given key in SODA_DATA_STRUCTURE with all the path of each file
    found in the current scope."""
    all_evaluated = False
    while(not all_evaluated):
        match_dolls = re.search(r"\$\{(.*?)\}", value)
        match_quest = re.search(r"\?\{(.*?)\}", value)
        if(match_dolls):
            value = evaluate_static_expression(value, match_dolls)
        if(match_quest):
            value = evaluate_dynamic_expression(value,
                                                match_quest, scope_expr)
        if(not match_dolls and not match_quest):
            all_evaluated = True

    return value


def from_yaml(yaml_dic, key):
    """Return the value (evaluated) corresponding to the key 'key'
    in the dictionnary yaml_dic"""
    try:
        value = yaml_dic[key]
    except KeyError:
        logging.error("YAML error: key '%s' not found.", key)
        return ""

    if isinstance(value, str):
        value = evaluate_yaml_expression(value)

    return value
