from src.eyesComment.smart_eyes import main
import logging


logger = logging.getLogger(__name__)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)-15s:%(levelname)s:%(name)s:%(message)s',
    )
    main()