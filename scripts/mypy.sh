#!/bin/sh
set -e

pip3 install returns

mypy . --config-file=mypy.ini
