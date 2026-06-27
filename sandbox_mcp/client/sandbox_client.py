import abc
from typing import Tuple

class BaseSandboxClient(abc.ABC):
    """Abstract interface for all agent execution environments."""

    @abc.abstractmethod
    async def read_file(self, path: str) -> str:
        """Reads a file from the sandbox."""
        pass

    @abc.abstractmethod
    async def write_file(self, path: str, content: str) -> bool:
        """Writes content to a file in the sandbox."""
        pass

    @abc.abstractmethod
    async def exec_shell(self, command: str, cwd: str = "/home/gem/workspace", new_session: bool = True) -> Tuple[bool, str, str]:
        """
        Executes a command, streams output to stdout, and returns the final state.
        Returns: (success_boolean, complete_stdout, complete_stderr)
        """
        pass
