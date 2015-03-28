import builtins

import logging
import sys
import re
from pprint import pformat

try:
    import yaml
except ImportError as err:
    print("Soda need pyyaml", err)


def load_yaml(yaml_filename):
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
    try:
        with builtins.SODA_LOCK:
            yaml.dump(to_dump, open(yaml_filename, 'w'),
                      default_flow_style=False)
    except (yaml.error.YAMLError) as yamlerr:
        logging.error("Dumping %s in %s: %s:",
                      yaml_filename,
                      to_dump,
                      yamlerr)


def evaluate_yaml_expression(value, files_in_current_scope=[]):
    all_evaluated = False
    while(not all_evaluated):
        match_dolls = re.search(r"\$\{(.*?)\}", value)
        match_quest = re.search(r"\?\{(.*?)\}", value)
        if(match_dolls):
            value = re.sub(r"\$\{" + match_dolls.group(1) + r"\}",
                           from_yaml(builtins.SODA_DATA_STRUCTURE,
                                     match_dolls.group(1)),
                           value)
        if(match_quest):
            to_evaluate = from_yaml(builtins.SODA_DATA_STRUCTURE,
                                    match_quest.group(1))
            evaluated_value = set()

            for serie in files_in_current_scope:
                match_eval = re.search(to_evaluate, serie)
                if match_eval:
                    evaluated_value.add(match_eval.group(0))
            try:
                assert len(evaluated_value) == 1
            except AssertionError:
                logging.critical("Bad evaluation of '?{%s}', within '%s'\n"
                                 "Matches are:\n%s",
                                 match_quest.group(1),
                                 value,
                                 pformat(evaluated_value))

            new = evaluated_value.pop()
            value = re.sub(r"\?\{" + match_quest.group(1) + r"\}", new, value)
        if(not match_dolls and not match_quest):
            all_evaluated = True

    return value


def from_yaml(yaml_dic, key):
    try:
        value = yaml_dic[key]
    except KeyError:
        logging.critical("YAML error: unable to found %s key", key)

    if isinstance(value, str):
        value = evaluate_yaml_expression(value)

    return value
