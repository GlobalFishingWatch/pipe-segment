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
