#!/usr/bin/env bash
set -e

echo Setting up virtual env...
python3 -m venv .venv

echo Activating venv
source .venv/bin/activate

echo Installing dependencies
pip install -r requirements.txt

echo Done
