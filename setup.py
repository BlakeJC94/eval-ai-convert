"""Setup script.

Install package:
    $ pip install -e .

Requirements can be updated using `pip-compile`:
    $ pip install pip-tools
    $ pip-compile
"""

from setuptools import setup, find_packages

setup(
    name="ea_convert",
    version="0.0.1",
    description='Python package for converting Eval AI dataset',
    url="https://github.com/BlakeJC94/eval-ai-convert",
    author="BlakeJC94",
    python_requires=">=3.8",
    packages=find_packages(),
    install_requires=[
        'numpy',
        'pandas',
        'mne',
        'tqdm',
        'pyarrow',
        'fire',
    ],
    entry_points={
        'console_scripts': ['ea-convert=ea_convert.__main__:main'],
    },
)
