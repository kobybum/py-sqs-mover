
test-and-lint:
	@pytest
	@mypy --ignore-missing-imports .
	@black --check -l 100 .
