import argparse
import logging
from repository.main import db_init, db_seed, db_drop, db_rebuild, db_views, db_build
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
    x = subparsers.add_parser(
        "db_seed", help="Fills all tables of the database"
    )
    x.add_argument('--table', help='testest')
    x.set_defaults(func=db_seed)
    subparsers.add_parser(
        "db_test", help="Test if the database is working"
    ).set_defaults(func=db_test)
    subparsers.add_parser(
        "db_drop", help="Drops all database tables"
    ).set_defaults(func=db_drop)
    subparsers.add_parser(
        "db_rebuild", help="Drops, inits and seeds the database"
    ).set_defaults(func=db_rebuild)
    subparsers.add_parser(
        "db_views", help="Creates the needed materialized views"
    ).set_defaults(func=db_views)
    subparsers.add_parser(
        "db_build", help="inits, seeds and creates views"
    ).set_defaults(func=db_build)

    args = parser.parse_args()

    if not hasattr(args, "func"):
        parser.print_help()
        return
    args.func()


if __name__ == "__main__":
    main()
