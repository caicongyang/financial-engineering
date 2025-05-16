from setuptools import setup, find_packages

setup(
    name="financial-engineering",
    version="0.1.0",
    packages=find_packages(where="program/python"),
    package_dir={"": "program/python"},
    install_requires=[
        'pandas',
        'sqlalchemy',
        'akshare',
        'pymysql',
        'schedule'
    ],
    python_requires='>=3.6',
) 