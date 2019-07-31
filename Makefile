.PHONY: prepare-dev
prepare-dev:
	@echo "Preparing your development environment..."; \
	PIPENV_VENV_IN_PROJECT=1 pipenv install --deploy --dev

.PHONY: clean-pyc
clean-pyc:
	@tput bold; echo "Cleaning __pycache__"; tput sgr0
	@find . -type d \( ! \( -path ./.venv -prune \) \) -name "__pycache__" -exec rm -rf {} +
	@tput bold; echo "Done cleaning __pycache__"; tput sgr0

.PHONY: clean-build
clean-build:
	@tput bold; echo "Cleaning build files..."; tput sgr0 \
	&& rm -rf build/ \
	&& rm -rf dist/ \
	&& rm -rf *.egg-info
	@tput bold; echo "Done cleaning build files..."; tput sgr0

.PHONY: lint
lint:
	@tput bold; echo "Running code style checker..."; tput sgr0; \
	pipenv run flake8 -v
	@tput bold; echo "Running linter..."; tput sgr0; \
	pipenv run pylint -E *.py