import fire

from . import ambtimes
from . import augment
from . import convert
from . import split
from . import label


def main():
    fire.Fire({
        'convert': convert,
        'ambtimes': ambtimes,
        'split': split,
        'label': label,
        'augment': augment,
    })


if __name__ == '__main__':
    main()
