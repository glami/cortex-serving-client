#!/usr/bin/env bash
set -uxe;
cd "$(dirname "$0")";

rm -r dist/ build/ cortex_serving_client.egg-info/;
python3 setup.py sdist bdist_wheel;
python3 -m twine upload dist/* --username __token__;