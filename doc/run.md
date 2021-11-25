# How to run the program

## Constraints

This project must be executed in unix environment, since the `zqm` does not support windows.

## Virtual environment

Make sure you have `python 3` and `pipenv` installed as a global dependecy.

To install the dependencies (from `src/`):

```bash
source pipenv install
```

To activate the python environment that is used in the project run the following command (from `src/`):

```bash
source pipenv shell
```

## Compilation

No compilation is needed for Python.

## How to execute

To run the project the only necessary command is:

```bash
python -m service [server | subscriber <messages_filename> <id>| publisher <topics_filename> <id>]
```
