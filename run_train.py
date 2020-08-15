from src.smart_features.eyesComment.train import main
import logging

logger = logging.getLogger(__name__)


def _main():
    main()

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)-15s:%(levelname)s:%(name)s:%(message)s',
    )
    _main()