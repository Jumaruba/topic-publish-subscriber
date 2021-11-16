from __future__ import annotations

import sys

from .programs.program import Program
from .programs.publisher import Publisher
from .programs.server import Server
from .programs.subscriber import Subscriber


def print_error(message: str):
    sys.stdout.write('\033[1;31m')
    print(message)
    sys.stdout.write('\033[0;0m')
    exit()


def get_program(args: list) -> Program | None:
    type_of_program = args[0]

    if type_of_program == 'server':
        return Server()

    if len(args) < 2:
        return None

    if type_of_program == 'publisher':
        return Publisher(args[1])
    elif type_of_program == 'subscriber':
        return Subscriber(args[1])
    else:
        return None


if __name__ == '__main__':
    "<program path> <subscriber|publisher|server>"

    if len(sys.argv) < 2:
        print_error("Invalid number of arguments, expected: server | subscriber <topics> | publisher <messages>")

    program = get_program(sys.argv[1:])
    if program is None:
        print_error("Invalid arguments, expected: server | subscriber <topics> | publisher <messages>")

    program.run()
