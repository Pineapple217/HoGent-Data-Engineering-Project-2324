import argparse
import logging
from repository.main import db_init, db_seed
from test.main import db_test

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(module)s | %(levelname)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="DEP Cli die heel cool is")
    subparsers = parser.add_subparsers(dest="Command")
    subparsers.add_parser("db_init", help="Create database and tables").set_defaults(
        func=db_init
    )
    subparsers.add_parser(
        "db_seed", help="Fills all tables of the database"
    ).set_defaults(func=db_seed)
    subparsers.add_parser(
        "db_test", help="Drops all tables and recreates them"
    ).set_defaults(func=db_test)
    args = parser.parse_args()

    if not hasattr(args, "func"):
        parser.print_help()
        return
    args.func()


if __name__ == "__main__":
    main()
