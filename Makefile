
test-and-lint:
	@pytest --disable-warnings
	@mypy --ignore-missing-imports . --exclude build
	@black --check -l 100 .
	@flake8 . --max-line-length=100 --exclude .venv

format-code:
	@black -l 100 .

prepare-egg-info:
	python setup.py test

prepare-workspace:
	rtx activate
	pip install -r requirements.txt -r requirements-dev.txt