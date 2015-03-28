#!/usr/bin/env python3

"""soda.

Usage:
    soda [-l <log_level> | --log <log_level>]
         [-w <workers> | --workers <workers>]
         <pipe.yaml> [<other_pipes.yaml>...]
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
        A yaml document starting with the data structure description
        and describing the pipeline.
    <other_pipes.yaml>...
        An  ordered list of yaml documets describing the pipeline.

    -c --clean
        Clean  current directory from soda generated files.

    -h --help
        Show this screen.

    -v --version
        Show version.
"""


def main(arguments):
    from soda_clean.soda_clean import clean
    from soda_process.soda_process import submit_process
    from soda_yaml.soda_yaml import load_all_yaml
    from soda_yaml.soda_yaml import from_yaml

    import os
    import sys
    from pprint import pformat
    import re
    import logging

    # ##############################################################################
    # GLobalize between modules:
    # ##############################################################################

    import builtins
    builtins.SODA_PATIENTS = set()
    builtins.SODA_STUDIES = set()
    builtins.SODA_SERIES = set()
    builtins.SODA_DATA_STRUCTURE = dict()
    builtins.SODA_LOG_FILENAME = "soda.log"
    builtins.SODA_STATE_DIR = ".soda"
    builtins.SODA_MAXWORKERS = 0
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

    logging.basicConfig(filename=builtins.SODA_LOG_FILENAME, level=numeric_level,
                        format='%(levelname)s @ %(asctime)s '
                        '->\n %(message)s\n',
                        datefmt='%m/%d/%Y %I:%M:%S %p')
    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.ERROR)
    # set a format which is simpler for console use
    formatter = logging.Formatter('[%(levelname)s]: %(message)s')
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
    builtins.SODA_DATA_STRUCTURE = yaml_pipe_document.pop(0)
    logging.debug("'DATA_STRUCTURE' used:\n%s",
                  pformat(builtins.SODA_DATA_STRUCTURE))
    for other_pipe in arguments['<other_pipes.yaml>']:
        yaml_pipe_document += load_all_yaml(other_pipe)

    # ##############################################################################
    # construct Data structure
    # ##############################################################################

    builtins.SODA_ROOT = from_yaml(builtins.SODA_DATA_STRUCTURE, '_ROOT')

    for root, folder, files in os.walk(builtins.SODA_ROOT):
        for f in files:
            path = os.path.join(root, f)

            for patient in from_yaml(builtins.SODA_DATA_STRUCTURE,
                                     '_PATIENTS'):
                patient_pattern = from_yaml(builtins.SODA_DATA_STRUCTURE,
                                            patient)
                match = re.search(r".*?" + patient_pattern, path)
                if match:
                    builtins.SODA_PATIENTS.add(match.group(0))

                for study in from_yaml(builtins.SODA_DATA_STRUCTURE,
                                       '_STUDIES'):
                    study_pattern = (patient_pattern + r".*?" +
                                     from_yaml(builtins.SODA_DATA_STRUCTURE,
                                               study))
                    match = re.search(r".*?" + study_pattern, path)
                    if match:
                        builtins.SODA_STUDIES.add(match.group(0))

                    for serie in from_yaml(builtins.SODA_DATA_STRUCTURE,
                                           '_SERIES'):
                        serie_pattern = (study_pattern + r".*?" +
                                         from_yaml(builtins.SODA_DATA_STRUCTURE,
                                                   serie))
                        match = re.search(r".*?" + serie_pattern, path)
                        if match:
                            builtins.SODA_SERIES.add(match.group(0))

    patients = sorted(builtins.SODA_PATIENTS)
    studies = sorted(builtins.SODA_STUDIES)
    series = sorted(builtins.SODA_SERIES)
    logging.debug("patients list:\n %s", pformat(patients))
    logging.debug("studies list:\n %s", pformat(studies))
    logging.debug("series list:\n %s", pformat(series))

    logging.debug("pipeline :\n%s", pformat(yaml_pipe_document))

    # ##############################################################################
    # Submit process
    # ##############################################################################

    for pipe_step_doc in yaml_pipe_document:
        try:
            scope = from_yaml(pipe_step_doc, '_SCOPE')
            assert scope in ['_PATIENTS', '_STUDIES', '_SERIES', '_ROOT']
        except:
            logging.error("Bad pipeline configuration file.\n"
                          "Each pipeline need a _SCOPE feild: "
                          "['_PATIENTS', '_STUDIES' '_SERIES' or '_ROOT']")

        if (scope == '_PATIENTS'):
            submit_process(patients, pipe_step_doc)
        elif (scope == '_STUDIES'):
            submit_process(studies, pipe_step_doc)
        elif (scope == '_SERIES'):
            submit_process(series, pipe_step_doc)
        elif (scope == '_ROOT'):
            submit_process([builtins.SODA_ROOT], pipe_step_doc)

    # # -- End of main


# -- Main
if __name__ == '__main__':
    try:
        from docopt import docopt
    except ImportError as err:
        print("Soda need docopt", err)

    arguments = docopt(__doc__, version='soda 1.0')
    main(arguments)
