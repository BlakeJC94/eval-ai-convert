"""Setup script.

Install package:
    $ pip install -e .

Requirements can be updated using `pip-compile`:
    $ pip install pip-tools
    $ pip-compile
"""

from setuptools import setup, find_packages

setup(
    name="eadata",
    version="0.7.1",
    description='Python package for converting Eval AI dataset',
    url="https://github.com/BlakeJC94/eval-ai-convert",
    author="BlakeJC94",
    python_requires=">=3.8",
    packages=find_packages(),
    install_requires=[
        'fire',
        'mne',
        'numpy',
        'pandas',
        'pyarrow',
        'tqdm',
    ],
    entry_points={
        'console_scripts': ['eadata=eadata.__main__:main'],
    },
)
