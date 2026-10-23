"""Scraper command line: ``python -m app.cli.scrape [--limit N] [--workers N] ...``."""

from app.services.scrape import main

if __name__ == "__main__":
    main()
