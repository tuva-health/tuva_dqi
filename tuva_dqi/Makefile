.PHONY: lint format run clean error

error:
	@echo "Available commands: lint, format, run, clean, error"
	@exit 2

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
