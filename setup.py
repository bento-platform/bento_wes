#!/usr/bin/env python

import setuptools

with open("README.md", "r") as rf:
    long_description = rf.read()

setuptools.setup(
    name="chord_wes",
    version="0.1.0",

    python_requires=">=3.6",
    install_requires=[
        "celery[redis]==4.4.0",
        "chord_lib[flask]==0.5.0",
        "Flask>=1.1,<2.0",
        "requests>=2.23,<3.0",
        "requests-unixsocket>=0.2.0,<0.3.0",
        "toil@git+https://github.com/DataBiosphere/toil@master"
    ],

    author="David Lougheed",
    author_email="david.lougheed@mail.mcgill.ca",

    description="Workflow execution service for CHORD.",
    long_description=long_description,
    long_description_content_type="text/markdown",

    packages=setuptools.find_packages(),
    include_package_data=True,

    url="https://github.com/c3g/chord_wes",
    license="LGPLv3",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)",
        "Operating System :: OS Independent"
    ]
)
