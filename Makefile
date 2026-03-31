.PHONY: build deploy clean all

# Default target (just run "make")
all: build

# Build source distribution + wheel
build:
	python3.9 -m build

# Upload to PyPI (or custom repository)
deploy:
	twine upload --repository terna-py --skip-existing dist/* --verbose

# Clean build artifacts
clean:
	rm -rf dist/ build/ *.egg-info/ __pycache__/

# Optional: Clean and rebuild
rebuild: clean build
