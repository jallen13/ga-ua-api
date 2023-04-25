from setuptools import setup, find_packages

VERSION = '0.0.1'
DESCRIPTION = 'Fetch GA UA data from the v4 api'
LONG_DESCRIPTION = 'A simple function to fetch GA UA data from the v4 api with custom logic to combat sampling and increase metrics beyond the 10 limit'

setup(
        name="ga_ua_api",
        version=VERSION,
        author="John Allen",
        author_email="john.allen@resolute.com",
        description=DESCRIPTION,
        long_description=LONG_DESCRIPTION,
        packages=find_packages(),
        install_requires=["pandas", "gaapi4py @ git+https://github.com/jallen13/gaapi4py.git"],
        classifiers= [
            "Development Status :: 4 - Beta",
            "Programming Language :: Python :: 3",
            "Operating System :: MacOS :: MacOS X",
            "License :: OSI Approved :: MIT License"
        ]
)