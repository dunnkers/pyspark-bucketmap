#!/usr/bin/env python

from setuptools import find_packages, setup

with open("README.md", "r") as fh:
    LONG_DESC = fh.read()
    setup(
        name="pyspark-bucketmap",
        version="0.0.5",
        py_modules=['pyspark_bucketmap'],
        scripts=['pyspark_bucketmap.py'],
        description="Easily group pyspark data into buckets and map them to different values.",
        keywords="",
        long_description=LONG_DESC,
        long_description_content_type="text/markdown",
        license="MIT",
        license_file="LICENSE",
        author="Jeroen Overschie",
        author_email="jeroen@darius.nl",
        maintainer="Jeroen Overschie",
        maintainer_email="jeroen@darius.nl",
        url="https://github.com/dunnkers/pyspark-bucketmap",
        project_urls={
            "Github": "https://github.com/dunnkers/pyspark-bucketmap",
            "Bug Tracker": "https://github.com/dunnkers/pyspark-bucketmap/issues",
            "Documentation": "https://dunnkers.com/pyspark-bucketmap",
        },
        include_package_data=True,
        install_requires=[
            "pyspark>=1.4.0",
            "overrides>=4.0.0",
            "numpy>=1.19.0"
        ],
        python_requires=">= 3.7",
        setup_requires=[],
        tests_require=["pytest>=6"],
        classifiers=[
            "License :: OSI Approved :: MIT License",
            "Development Status :: 4 - Beta",
            "Programming Language :: Python :: 3.7",
            "Programming Language :: Python :: 3.8",
            "Programming Language :: Python :: 3.9",
            "Operating System :: POSIX :: Linux",
            "Operating System :: MacOS",
            "Operating System :: Microsoft :: Windows",
            "Typing :: Typed",
            "Topic :: Scientific/Engineering",
            "Topic :: Software Development",
            "Topic :: Software Development :: Libraries :: Python Modules",
        ],
    )
