.PHONY: help install scrape link links

PY ?= python3
PIP ?= pip

.DEFAULT_GOAL := help

help:
	@echo "Targets:"
	@echo "  install            Install Python dependencies"
	@echo "  scrape             Run full scraper (pass ARGS=...)"
	@echo "  link               Print report URL for a meeting (MEETING_ID=...)"
	@echo "  links              Dump all meeting->report URLs (OUTPUT=links.json optional)"
	@echo ""
	@echo "Examples:"
	@echo "  make install"
	@echo "  make scrape ARGS=--debug"
	@echo "  make link MEETING_ID=1e4227e0-7c1f-4a8e-8c96-5d0e35fb8b9a"
	@echo "  make links OUTPUT=links.json"

install:
	$(PIP) install -r requirements.txt

scrape: install
	@mkdir -p output
	$(PY) scrape.py $(ARGS)

link: install
	@if [ -z "$(MEETING_ID)" ]; then \
		echo "MEETING_ID is required. Usage: make link MEETING_ID=<uuid>"; \
		exit 1; \
	fi
	$(PY) extract_link.py --meeting-id $(MEETING_ID)

links: install
	@if [ -n "$(OUTPUT)" ]; then \
		$(PY) extract_link.py --all --output $(OUTPUT); \
	else \
		$(PY) extract_link.py --all; \
	fi

