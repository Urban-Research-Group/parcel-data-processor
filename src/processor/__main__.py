"""Entry point for the application"""

import argparse
from run import run


def _parse_args():
    desc = "Execute a defined set of operations to process data from many files"
    arg_and_msg = {
        "config_path": "Path to the configuration file",
        "exec_name": "Name given to current execution",
    }

    parser = argparse.ArgumentParser(description=desc)
    for arg, msg in arg_and_msg.items():
        parser.add_argument(arg, type=str, help=msg)

    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run(args.config_path, args.exec_name)
