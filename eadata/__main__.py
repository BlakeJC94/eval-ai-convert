import fire

from . import ambtimes
from . import convert
from . import split


def main():
    fire.Fire({
        'convert': convert,
        'ambtimes': ambtimes,
        'split': split,
    })


if __name__ == '__main__':
    main()
