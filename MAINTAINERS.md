# How To Release A New Version

Warning: Below is an experimental version. Automation will be added later.

1. Read [official how-to](https://packaging.python.org/guides/distributing-packages-using-setuptools/).
1. Re-test the project.
1. Make git-pull to make sure on latest sources.
1. Install reqs:

    ``` pip install -r requirements-dev.txt ```

1. Select package version new_version where major and minor version should be equal to the supported cortex version and patch version is a custom version specific to this package.
   
1. Update and commit package version.

    ` vi setup.py `

1. Update README.md to the newest version.

   
1. Execute and use your API token as a password:
   ```
   bash ./publish_to_pypi.sh ${new_version}; 
   ```
   
- TODO GPG signed Tag? https://github.com/scikit-build/ninja-python-distributions/blob/master/docs/make_a_release.rst
