import argparse
from abc import ABC, abstractmethod


class Command(ABC):
    """Base class for CLI commands."""
    @classmethod
    @abstractmethod
    def add_to_subparsers(cls, subparsers):
        """Add command-line options to a subparses action."""

    @classmethod
    @abstractmethod
    def run(cls, **kwargs):
        """Runs the implemented command"""

    @staticmethod
    def formatter():
        def argparse_formatter(prog):
            return argparse.HelpFormatter(prog, max_help_position=50)

        return argparse_formatter
