import sys
import logging
import argparse

from rich.logging import RichHandler


logger = logging.getLogger(__name__)


APP_NAME = 'PIPE'
APP_DESCRIPTION = 'Executes a GFW pipeline.'
APP_LOG_FORMAT = '%(name)s - %(message)s'
APP_COMMANDS = {}


def setup_logger(log_format):
    logging.basicConfig(
        level=logging.INFO, format=log_format, handlers=[RichHandler(level="NOTSET")])

    # Hide logging from external libraries
    external_libs = []
    for lib in external_libs:
        logging.getLogger(lib).setLevel(logging.ERROR)


class APPError(Exception):
    pass


class APP:
    def __init__(self, commands):
        self.parser = argparse.ArgumentParser(prog=APP_NAME, description=APP_DESCRIPTION)
        self.subparsers = self.parser.add_subparsers(dest='command', help='available commands')

        self.commands = commands

        for command in self.commands.values():
            command.add_to_subparsers(self.subparsers)

    def parse_args(self, args):
        self.args = self.parser.parse_args(args=args or ['--help'])

        if self.args.command not in self.commands:
            raise APPError("Invalid command: {}".format(self.command))

        self.command = self.commands[self.args.command]

        if self.args.verbose:
            logging.getLogger().setLevel(logging.DEBUG)

    def execute(self):
        return self.command.run(**self._get_command_args())

    def _get_command_args(self):
        command_args = vars(self.args)
        del command_args['command']
        del command_args['verbose']

        return command_args


def run(args):
    setup_logger(APP_LOG_FORMAT)

    try:
        logger.info(f"Initializing {APP_NAME}")
        app = APP(APP_COMMANDS)

        logger.info(f"Parsing {APP_NAME} arguments...")
        app.parse_args(args)

        logger.info(f"Running {APP_NAME} command: {app.args.command}")
        app.execute()
    except APPError as e:
        app.logger.error(e)


def main():
    run(sys.argv[1:])


if __name__ == '__main__':
    main()
