from src.eyesComment.preprocess import main
import logging


def _main():
    main()


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)-15s:%(levelname)s:%(name)s:%(message)s',
    )
    _main()
