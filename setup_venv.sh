#!/bin/bash

deactivate || true
rm -fr ./.venv || true
python -m venv ./.venv && source ./.venv/bin/activate && python -m pip install -r requirements.txt