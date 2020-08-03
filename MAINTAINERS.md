# How To Release A New Version

Warning: Below is an experimental version. Automation will be added later.

1. Read [official how-to](https://packaging.python.org/guides/distributing-packages-using-setuptools/).
1. Install reqs

    ``` pip install -r requirements-dev.txt ```

1. Update and commit version.

    ` vi setup.py `
    
1. Generate distribution files.

    `python3 setup.py sdist bdist_wheel`
    
1. Upload using below username is `__token__` and password is your API token.

    `python3 -m twine upload dist/*`
