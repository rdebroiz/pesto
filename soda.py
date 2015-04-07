#!/usr/bin/env python3

"""soda.

Usage:
    soda [-l <log_level> | --log <log_level>]
         [-w <workers> | --workers <workers>]
         <pipe.yaml>
    soda -c | --clean
    soda -h | --help
    soda -v |--version

Options:
    -w --workers <workers>
        Number max of different processus to launch together.
        [default: 0] -> Number of host's CPU.
    -l --log <log_level>
        Level of verbosity to print in the log file.
        Must be in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        [default: INFO]
    <pipe.yaml>
        A yaml file starting with the data structure description
        and describing the pipeline.

    -c --clean
        Clean  current directory from soda generated files.

    -h --help
        Show this screen.

    -v --version
        Show version.
"""


def main(arguments):
    """Main function"""
    from soda_clean.soda_clean import clean
    from soda_process.soda_process import submit_process
    from soda_yaml.soda_yaml import load_all_yaml
    from soda_yaml.soda_yaml import from_yaml
    from soda_yaml.soda_yaml import escape_reserved_re_char

    import os
    import sys
    from pprint import pformat
    import re
    import logging

    # ##############################################################################
    # GLobalize between modules:
    # ##############################################################################

    import builtins
    builtins.SODA_DATA_STRUCTURE = dict()
    builtins.SODA_LOG_FILENAME = "soda.log"
    builtins.SODA_STATE_DIR = ".soda"
    builtins.SODA_MAXWORKERS = 0
    builtins.SODA_ROOT = ""
    builtins.SODA_FILES_IN_ROOT = list()
    from concurrent.futures.thread import threading
    builtins.SODA_LOCK = threading.Lock()

    # ##############################################################################
    # Clean ?
    # ##############################################################################

    if arguments['--clean']:
        clean()

    # ##############################################################################
    # We need this dir.
    # ##############################################################################

    os.makedirs(builtins.SODA_STATE_DIR, exist_ok=True)

    # ##############################################################################
    # Setup logging
    # ##############################################################################

    log_level = arguments['--log']

    # assuming loglevel is bound to the string value obtained from the
    # command line argument. Convert to upper case to allow the user to
    # specify --log=DEBUG or --log=debug
    numeric_level = getattr(logging, log_level.upper(), None)

    try:
        if not isinstance(numeric_level, int):
            assert ValueError('Invalid log level: %s' % log_level)
    except ValueError as err:
        print(err)
        sys.exit(1)

    logging.basicConfig(filename=builtins.SODA_LOG_FILENAME,
                        level=numeric_level,
                        format='%(levelname)s @ %(asctime)s '
                        '->\n %(message)s\n',
                        datefmt='%m/%d/%Y %I:%M:%S %p')
    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.ERROR)
    # set a format which is simpler for console use
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    formatter = logging.Formatter(BOLD + '[%(levelname)s]: %(message)s' + ENDC)
    # tell the handler to use this format
    console.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger('').addHandler(console)

    logging.info("\n"
                 "######################################################\n"
                 "######################################################\n"
                 "######################################################\n"
                 "\nStart SODA")
    logging.debug("Soda arguments:\n%s", pformat(arguments))

    # ##############################################################################
    # Get number of workers to use:
    # ##############################################################################

    try:
        builtins.SODA_MAXWORKERS = arguments['--workers']
        builtins.SODA_MAXWORKERS = int(builtins.SODA_MAXWORKERS)
        assert builtins.SODA_MAXWORKERS >= 0
    except (ValueError, AssertionError):
        logging.warning("<workers> must be a positive integer.\nDefault "
                        "value (auto-determine by the system) will be "
                        "used.\n")
        builtins.SODA_MAXWORKERS = 0

    try:
        if builtins.SODA_MAXWORKERS == 0:
            builtins.SODA_MAXWORKERS = os.cpu_count()
        assert builtins.SODA_MAXWORKERS is not None
    except AssertionError as err:
        logging.critical("Could not find host's number of CPU please set the "
                         "--workers option yourself", err)
        sys.exit(1)

    # ##############################################################################
    # Get Pipe document
    # ##############################################################################

    yaml_pipe_document = load_all_yaml(arguments['<pipe.yaml>'])
    try:
        builtins.SODA_DATA_STRUCTURE = yaml_pipe_document.pop(0)
    except IndexError:
        logging.critical("Empty <pipe.yaml> file.")
        sys.exit(1)

    logging.debug("'DATA_STRUCTURE' used:\n%s",
                  pformat(builtins.SODA_DATA_STRUCTURE))

    steps = list()
    for step in yaml_pipe_document:
        try:
            if ('__FILE__' in step):
                pipe_path = step['__FILE__']
                # pipe_path is not absolu
                if(not os.path.exists(pipe_path)):
                    base = os.path.dirname(arguments['<pipe.yaml>'])
                    pipe_path = os.path.join(base, pipe_path)

                # we tried to get an abs path from basedir of <pipe.yaml>
                # and the given path.
                if(not os.path.exists(pipe_path)):
                    logging.critical("'%s' not found.", step['__FILE__'])
                    sys.exit(1)

                steps += load_all_yaml(pipe_path)
            else:
                steps += step
        except TypeError:
            if(step is None):
                logging.critical("Bad pipeline document.")
                sys.exit(1)

    # ##############################################################################
    # construct Data structure
    # ##############################################################################

    builtins.SODA_ROOT = from_yaml(builtins.SODA_DATA_STRUCTURE, '__ROOT__')

    files_set = set()
    exprs_for_scope = dict()
    for root, folder, files in os.walk(builtins.SODA_ROOT):
        for f in files:
            path = os.path.join(root, f)
            files_set.add(path)
    builtins.SODA_FILES_IN_ROOT = sorted(files_set)

    scopes = from_yaml(builtins.SODA_DATA_STRUCTURE, '__SCOPES__')
    for scope in scopes:
        scope_expr_set = set()
        pattern = from_yaml(scopes, scope)
        for f in builtins.SODA_FILES_IN_ROOT:
            try:
                match = re.search(r".*?" + pattern, f)
            except re.error as err:
                logging.critical("Bad regular expression to describe scope: "
                                 "'%s'\nerr: %s", scope, err)
                sys.exit(1)
            if match:
                scope_expr = match.group(0)
                scope_expr = escape_reserved_re_char(scope_expr)
                scope_expr_set.add(scope_expr)
        exprs_for_scope[scope] = sorted(scope_expr_set)

    logging.debug("scopes expressions:\n%s", pformat(exprs_for_scope))
    logging.debug("files:\n%s", pformat(builtins.SODA_FILES_IN_ROOT))
    logging.debug("pipeline :\n%s", pformat(steps))

    # ##############################################################################
    # Submit process
    # ##############################################################################

    # check pipeline integrity:
    for step_document in steps:
        try:
            scope = from_yaml(step_document, '__SCOPE__')
            description = from_yaml(step_document, '__DESCRIPTION__')
            name = from_yaml(step_document, '__NAME__')
            cmd = from_yaml(step_document, '__CMD__')
            assert(scope is not None and description is not None
                   and name is not None and cmd is not None)
            assert(scope is not "" and description is not ""
                   and name is not "" and cmd is not [])
            if scope not in exprs_for_scope.keys():
                logging.critical("Bad pipeline configuration file.\n"
                                 "Each of your pipeline step need a __SCOPE__ "
                                 "feild with a value within: %s",
                                 exprs_for_scope.keys())
                sys.exit(1)
        except (TypeError, AssertionError):
            logging.critical("Probleme detected at pipe step number %s,"
                             " be sure to have correct '__DESCRIPTION__'"
                             ", '__SCOPE__', '__NAME__', and '__CMD__' keys.",
                             steps.index(step_document))
            sys.exit(1)

    for step_document in steps:
        scope = from_yaml(step_document, '__SCOPE__')
        submit_process(exprs_for_scope[scope], step_document)

# -- Main
if __name__ == '__main__':
    import sys
    try:
        from docopt import docopt
    except ImportError as err:
        print("Soda need docopt", err)
        sys.exit(1)

    arguments = docopt(__doc__, version='soda 1.0')
    main(arguments)
