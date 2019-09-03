#!/usr/bin/env python

import setuptools

from chord_wes import __version__

with open("README.md", "r") as rf:
    long_description = rf.read()

setuptools.setup(
    name="chord_wes",
    version=__version__,

    python_requires=">=3.6",
    install_requires=["celery[redis]", "chord_lib @ git+https://bitbucket.org/genap/chord_lib", "Flask", "requests",
                      "toil[wdl]"],

    author="David Lougheed",
    author_email="david.lougheed@mail.mcgill.ca",

    description="Workflow execution service for CHORD.",
    long_description=long_description,
    long_description_content_type="text/markdown",

    packages=["chord_wes"],
    include_package_data=True,

    url="TODO",
    license="MIT",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent"
    ]
)