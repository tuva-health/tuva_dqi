.PHONY: lint format run clean

lint:
	ruff check .

format:
	ruff format .

run:
	python app.py

clean:
	rm -rf __pycache__
	rm -rf .pytest_cache
	rm -rf .coverage
