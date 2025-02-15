from setuptools import setup, find_packages

setup(
    name="financial-engineering",
    version="0.1",
    package_dir={"": "program/python"},
    packages=find_packages(where="program/python"),
) 