#!/usr/bin/env python3

"""pesto.

Usage:
    pesto [-l <log_level> | --log <log_level>]
         [-w <workers> | --workers <workers>]
         <pipe.yaml>
    pesto -c | --clean
    pesto -h | --help
    pesto -v |--version

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
        Clean  current directory from pesto generated files.

    -h --help
        Show this screen.

    -v --version
        Show version.
"""

import os
import logging
from pprint import pformat


def main(arguments):
    import log

    """Main function"""
    # ##############################################################################
    # clean ?
    # ##############################################################################

    # if arguments['--clean']:
    #     clean()

    # ##############################################################################
    # we need this dir.
    # ##############################################################################

    pesto_dir = os.path.join(os.curdir, '.pesto')
    pesto_log_file = os.path.join(pesto_dir, 'pesto.log')
    os.makedirs(pesto_dir, exist_ok=True)

    # ##############################################################################
    # setup logs
    # ##############################################################################

    log_level = arguments['--log']
    log.setup(pesto_log_file, log_level)
    logging.debug("cmd line arguments:\n%s", pformat(arguments))

    # ##############################################################################
    # get number of workers to use:
    # ##############################################################################

    try:
        max_workers = arguments['--workers']
        max_workers = int(max_workers)
        assert max_workers >= 0
    except (ValueError, AssertionError):
        logging.warning("<workers> must be a positive integer.\nDefault "
                        "value (auto-determine by the system) will be "
                        "used.\n")
        max_workers = 0

    try:
        if max_workers == 0:
            max_workers = os.cpu_count()
        assert max_workers is not None
    except AssertionError as err:
        logging.critical("could not find host's number of CPU please set the "
                         "--workers option yourself", err)
        sys.exit(1)

    # ##############################################################################
    # construct data model
    # ##############################################################################
    from yaml_io import YamlIO
    try:
        import path
    except ImportError:
        log.quit_with_error("Pesto requiered path.py to be installed, "
                            "checkout requirement.txt.")

    yaml_document_path = path.Path(arguments['<pipe.yaml>']).abspath()
    yaml_document = YamlIO.load_all_yaml(yaml_document_path)
    try:
        from data_model import DataModel
        DataModel(yaml_document.pop(0), yaml_document_path.dirname())
    except IndexError:
        logging.critical("empty <pipe.yaml> file.")
        sys.exit(1)

    # ##############################################################################
    # construct pipeline
    # ##############################################################################

    from pipeline import Pipeline
    Pipeline(yaml_document)

# -- Main
if __name__ == '__main__':
    import sys
    try:
        from docopt import docopt
    except ImportError as err:
        print("pesto need docopt", err)
        sys.exit(1)

    arguments = docopt(__doc__, version='pesto 0.1.0')
    main(arguments)
