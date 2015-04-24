import logging
import sys


def quit_with_error(msg):
    logging.critical(msg)
    sys.exit(1)


def setup(lof_file, lvl):
    # assuming loglevel is bound to the string value obtained from the
    # command line argument. Convert to upper case to allow the user to
    # specify --log=DEBUG or --log=debug
    numeric_level = getattr(logging, lvl.upper(), None)

    try:
        if not isinstance(numeric_level, int):
            assert ValueError('Invalid log level: %s' % lvl)
    except ValueError as err:
        print(err)
        sys.exit(1)

    logging.basicConfig(filename=lof_file,
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
    formatter = logging.Formatter(BOLD + '[%(levelname)s]: %(message)s\n' + ENDC)
    # tell the handler to use this format
    console.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger('').addHandler(console)

    logging.info("\n"
                 "######################################################\n"
                 "######################################################\n"
                 "######################################################\n"
                 "\nStart SODA")
