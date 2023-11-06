

all:
    $(info ==============================)

test_setp:
	./venv/bin/pip install pytest

test:
	./venv/bin/python3 -m pytest test/

.PHONY: all test
