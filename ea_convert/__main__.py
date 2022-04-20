import fire

from . import convert


def main():
    fire.Fire({
        'convert': convert,
    })


if __name__ == '__main__':
    main()
