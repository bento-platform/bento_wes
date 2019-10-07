#!/usr/bin/env python

import setuptools

with open("README.md", "r") as rf:
    long_description = rf.read()

setuptools.setup(
    name="chord_wes",
    version="0.1.0",

    python_requires=">=3.6",
    install_requires=["celery[redis]", "chord_lib @git+https://github.com/c3g/chord_lib", "Flask", "requests",
                      "toil[wdl]"],

    author="David Lougheed",
    author_email="david.lougheed@mail.mcgill.ca",

    description="Workflow execution service for CHORD.",
    long_description=long_description,
    long_description_content_type="text/markdown",

    packages=["chord_wes"],
    include_package_data=True,

    url="TODO",
    license="LGPLv3",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent"
    ]
)
