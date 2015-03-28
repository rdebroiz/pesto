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
        stream = open(yaml_filename, 'r')
        yaml_doc = yaml.load(stream)
        # stream.close()
    except OSError as oserr:
        logging.error("Loading %s: %s", yaml_filename, oserr)
        sys.exit(1)
    except (yaml.error.YAMLError,
            yaml.scanner.ScannerError,
            yaml.parser.ParserError) as yamlerr:
        logging.error("Loading %s: %s:", yaml_filename, yamlerr)
        sys.exit(1)
    return yaml_doc


def load_all_yaml(yaml_filename):
    try:
        stream = open(yaml_filename, 'r')
        yaml_docs = yaml.load_all(stream)
        # stream.close()
    except OSError as oserr:
        logging.error("Loading %s: %s", yaml_filename, oserr)
        sys.exit(1)
    except (yaml.error.YAMLError,
            yaml.scanner.ScannerError,
            yaml.parser.ParserError) as yamlerr:
        logging.error("Loading %s: %s:", yaml_filename, yamlerr)
        sys.exit(1)
    return list(yaml_docs)


def evaluate_yaml_expression(value, cur_series=[]):
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

            for serie in cur_series:
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


def from_yaml(dic, key):
    try:
        value = dic[key]
    except KeyError:
        logging.critical("YAML error: unable to found %s key", key)

    if isinstance(value, str):
        value = evaluate_yaml_expression(value)

    return value
