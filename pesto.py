import logging
import sys


def quit_with_error(msg=''):
    logging.critical(msg)
    sys.exit(1)
