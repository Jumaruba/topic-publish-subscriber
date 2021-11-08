# SDLE Project

SDLE Project for group T3G&lt;p&gt;&lt;q&gt;.

Group members:

1. Alexandre Abreu ([up201800168@up.pt](mailto:up201800168@up.pt))
2. Diana Freitas ([up201806230@up.pt](mailto:up201806230@up.pt))
3. Juliane Marubayashi ([up201800175@up.pt](mailto:up201800175@up.pt))
4. Simão Lúcio ([up201303845@up.pt](mailto:up201303845@up.pt))

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
python -m service <subscriber|publisher>
```
