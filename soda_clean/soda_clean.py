import builtins

import sys
import os


def clean():
    """Clean log files and state files."""
    if(os.path.exists(builtins.SODA_LOG_FILENAME)):
        os.remove(builtins.SODA_LOG_FILENAME)
    if(os.path.exists(builtins.SODA_STATE_DIR)):
        for file_ in os.listdir(builtins.SODA_STATE_DIR):
            os.remove(os.path.join(builtins.SODA_STATE_DIR, file_))
        os.rmdir(builtins.SODA_STATE_DIR)
    sys.exit(0)
