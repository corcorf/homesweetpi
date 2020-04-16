"""Console script for homesweetpi."""
import argparse
import sys
import logging

LOG = logging.getLogger("homesweetpi.cli")


def main():
    """Console script for homesweetpi."""
    LOG.debug("Running CLI main script")
    parser = argparse.ArgumentParser()
    parser.add_argument('_', nargs='*')
    args = parser.parse_args()

    print("Arguments: " + str(args._))
    print("Replace this message by putting your code into "
          "homesweetpi.cli.main")
    return 0


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
