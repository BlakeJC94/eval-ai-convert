import fire

from . import ambtimes
from . import convert


def main():
    fire.Fire({
        'convert': convert,
        'ambtimes': ambtimes,
    })


if __name__ == '__main__':
    main()
