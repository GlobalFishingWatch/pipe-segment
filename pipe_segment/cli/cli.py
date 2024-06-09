import sys
import logging
import argparse

from rich.logging import RichHandler

from .commands import Segment, SegmentIdentity

logger = logging.getLogger(__name__)


class CLI:
    """Generic command-line interface."""
    HELP_VERBOSE = 'whether to run with DEBUG log level (default: %(default)s).'
    HELP_LOG_FILE = "file to send logging output to."

    def __init__(self, args):
        self._args = args

        self._setup_logger()
        self._add_commands()
        self._parse_args()

    def execute(self):
        logger.info(f"Running pipe {self.command} command...")
        return self.execute_command(self.args, self.extra_args)

    def _add_commands(self):
        self.parser = argparse.ArgumentParser(
            prog=self.NAME, description=self.DESCRIPTION, formatter_class=self.formatter())

        self.parser.add_argument('-v', '--verbose', action='store_true', help=self.HELP_VERBOSE)
        self.parser.add_argument('-l', '--log_file', metavar='\b', help=self.HELP_LOG_FILE)

        self.subparsers = self.parser.add_subparsers(dest='command', help='available commands')

        for command in self.COMMANDS:
            command.add_to_subparsers(self.subparsers)

    def _parse_args(self):
        self.args, self.extra_args = self.parser.parse_known_args(args=self._args or ['--help'])

        if self.args.verbose:
            logging.getLogger().setLevel(logging.DEBUG)

        if self.args.log_file:
            logging.getLogger().addHandler(logging.FileHandler(self.args.log_file))

        self.execute_command = self.args.func
        self.command = self.args.command

        del self.args.verbose
        del self.args.log_file
        del self.args.command
        del self.args.func

    def _setup_logger(self):
        logging.basicConfig(
            level=logging.INFO,
            format=self.LOG_FORMAT, handlers=[RichHandler(level="NOTSET")], force=False)
        # force = True is needed because some other library is setting the root logger.

        for module in self.LOG_LEVEL_WARNING:
            logging.getLogger(module).setLevel(logging.WARNING)


class PIPE(CLI):
    NAME = 'PIPE'
    DESCRIPTION = 'Executes a GFW pipeline.'
    LOG_FORMAT = '%(name)s - %(message)s'

    # packages / moudules for which to set the log level as WARNING.
    LOG_LEVEL_WARNING = [
        "apache_beam.io.gcp",
    ]

    COMMANDS = [Segment, SegmentIdentity]

    @staticmethod
    def formatter():
        def formatter(prog):
            return argparse.RawTextHelpFormatter(prog, max_help_position=50)

        return formatter


def run(args):
    PIPE(args).execute()


def main():
    run(sys.argv[1:])


if __name__ == '__main__':
    main()
