
test-and-lint:
	@pytest
	@mypy --ignore-missing-imports .
	@black --check -l 100 .

format-code:
	@black -l 100 .
