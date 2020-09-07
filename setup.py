import setuptools

with open("requirements.txt", "r") as fs:
    reqs = [r for r in fs.read().splitlines() if (len(r) > 0 and not r.startswith("#"))]

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="cortex-serving-client",
    version="0.18.4",
    author="Vaclav Kosar, Antonin Hoskovec, Radek Bartyzal",
    author_email="vaclav.kosar@glami.cz, antonin.hoskovec@glami.cz, radek.bartyzal@glami.cz",
    description="Cortex.dev ML Serving Client for Python with garbage API collection.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/glami/cortex-serving-client",
    packages=setuptools.find_packages(exclude=("test*",)), install_requires=reqs,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: Utilities",
    ],
    python_requires='>=3.6',
)