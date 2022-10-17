from src.adapters.filesystems import LocalFilesystem
from src.logger import log


class PatchedLocalFilesystem(LocalFilesystem):
    # Patched version of the local filesystem that will write to memory rather than disk
    def __init__(self):
        self._written_files = {}
        self.real_local_filesystem = LocalFilesystem()

    def write_file(self, filepath: str, content: str) -> None:
        log.debug(f"TEST: Writing file '{filepath}' to memory")
        self._written_files[filepath] = content

    # Note this may fail if the requested path is a different to when it was written
    # (e.g. written as a relative path, then requested back via an absolute path}
    def read_file(self, filepath: str) -> str:
        log.debug(f"TEST: reading path {filepath}")
        if filepath in self._written_files.keys():
            return self._written_files[filepath]
        log.debug(
            f"TEST: Didn't find path in memory: {self._written_files.keys()}. Falling back to local"
        )
        return self.real_local_filesystem.read_file(filepath)
