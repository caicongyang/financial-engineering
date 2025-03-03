from setuptools import setup, find_packages

setup(
    name="financial-engineering",
    version="0.1",
    package_dir={"": "program/python"},
    packages=find_packages(where="program/python", exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
    install_requires=[
        'pandas',
        'sqlalchemy',
        'akshare',
        'pymysql',
        'schedule'
    ],
    python_requires='>=3.6',
) 