#!/usr/bin/env bash
set -uxe;
cd "$(dirname "$0")";


echo "INFO: Check no uncommitted changes";
if git diff HEAD --name-only|grep -v 'cortex_client\|test\|.ipynb'; then
  echo "There are uncommitted changes! Please stash them and try again.";
  exit 1;
fi;

tag="$1"
echo "Check tag in setup.py"
if ! grep -q "${tag}" setup.py; then
  echo "Tag is not configured in the setup.py";
  exit 1;
fi;

git tag "${tag}" || true;
git push --tags;

rm -r dist/ build/ cortex_serving_client.egg-info/ || true;
python3 setup.py sdist bdist_wheel;
python3 -m twine upload dist/* --username __token__;