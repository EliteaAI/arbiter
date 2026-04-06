# Testing app for arbiter

## Running tests

to run tests you need to execution `pytest -q tests/`

## Test coverage

to check test coverage you need to run
```
coverage run --source=arbiter -m pytest -q tests/
coverage report -m
coverage html
```
