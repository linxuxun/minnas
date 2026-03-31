.PHONY: test lint install clean build

install:
	pip install -e .

test:
	python -m pytest tests/ -v
	python -m unittest discover -s tests -v

test-verbose:
	python -m unittest discover -s tests -v

clean:
	rm -rf build/ dist/ *.egg-info/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type d -name "*.minnas" -exec rm -rf {} +

build:
	python setup.py sdist bdist_wheel

lint:
	python -m py_compile minnas/*.py

.DEFAULT_GOAL := install
