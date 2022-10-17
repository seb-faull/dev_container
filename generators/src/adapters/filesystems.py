import abc
from pathlib import Path
from src.logger import log
import os


class BaseFilesystem(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def posix_path(self, path: str) -> str:
        """
        Converts a given path string into a posix representation of that path.
        :param path: str; filepath or folderpath.
        :return: str; a string representation of the path in posix form.
        """
        pass

    @abc.abstractmethod
    def absolute_path(self, path: str) -> str:
        """
        Return the absolute path of a given path. Throw a KeyError if the path does not exist.
        :param path: str; a filepath or folderpath.
        :return: str; the absolute path reference to the given path.
        """
        pass

    @abc.abstractmethod
    def path_parent(self, path: str) -> str:
        """
        Return the parent of a given path, i.e. 'one layer up'. Throw a KeyError if the path does not exist.
        :param path: str; a filepath or folderpath.
        :return: str; the path to one level above the input path.
        """
        pass

    @abc.abstractmethod
    def filename(self, filepath: str) -> str:
        """
        Returns the full filename of a given filepath.
        :param filepath: str; the path to the file. Throws a TypeError if the path is not a filepath.
        :return: str; the full filename of the file e.g. my_sql_file.sql.
        """
        pass

    @abc.abstractmethod
    def filename_stem(self, filepath: str) -> str:
        """
        Returns the filename stem of a given filepath i.e. the filename without the file extension
        :param filepath: str; The path to the file. Throws a TypeError if the path is not a filepath.
        :return: str; the filename stem of the file e.g. my_sql_file
        """
        pass

    @abc.abstractmethod
    def filename_extension(self, filepath: str) -> str:
        """
        Returns the file extension of a given filepath.
        :param filepath: str; The path to the file. Throws a TypeError if the path is not a filepath.
        :return: str: The extension/s of the file e.g. .sql If there are multiple extensions, they will all be
        returned as a single string e.g. .tar.gz
        """
        pass

    @abc.abstractmethod
    def path_exists(self, path: str) -> bool:
        """
        Check whether a given path exists on the filesystem.
        :param path: str; a filepath or folderpath.
        :return: boolean; True when the given path exists on the filesystem, False otherwise.
        """
        pass

    @abc.abstractmethod
    def read_file(self, filepath: str) -> str:
        """
        Read the contents of a file at a given filepath. If the file does not exist, raises a KeyError if the
        file does not exist.
        :param filepath: str; a filepath. Raises a TypeError if a folderpath is passed.
        :return: str; the contents of the file as a string.
        """
        pass

    @abc.abstractmethod
    def is_directory(self, path: str) -> bool:
        """
        Check whether a given path is a directory.
        :param path: str; a filepath or folderpath. Throws a KeyError if the path does not exist.
        :return: bool; True if the path is a directory, False otherwise.
        """
        pass

    @abc.abstractmethod
    def list_files(self, folderpath: str) -> list[str]:
        """
        List the files within a given folder. Not recursive - files within sub-folders will not be listed.
        :param folderpath: str; Path to the relevant folder. Passing a filepath will throw a TypeError.
        Throws a KeyError if the path does not exist.
        :return: list[str]; a list containing any filenames within the given path
        e.g. ["file_a.py", "file_e.sql"].
        """
        pass

    @abc.abstractmethod
    def list_folders(self, folderpath: str) -> list[str]:
        """
        List the folders within a given folder. Not recursive - folders within sub-folders will not be
        listed.
        :param folderpath: str; Path to the relevant folder. Passing a filepath will throw a TypeError.
        Throws a KeyError if the path does not exist.
        :return: list[str]; a list containing any folders within the given path e.g. ["dir_a/", "dir_b/"]
        """
        pass

    @abc.abstractmethod
    def list_contents(self, folderpath: str) -> list[str]:
        """
        List the files and folders within a given path. Not recursive - files & folders within sub-folders
        will not be listed
        :param folderpath: str; Path to the relevant folder. Passing a filepath will throw a TypeError.
        Throws a KeyError if the path does not exist.
        :return: list[str]; a list containing any filenames or folders within the given path e.g. ["file_a.py",
        "file_e.sql", "dir_a/", "dir_b/"].
        """
        pass

    @abc.abstractmethod
    def write_file(self, filepath: str, content: str) -> None:
        """
        Write file content to a given filepath. Will overwrite any current file content.
        :param filepath: str; Path to the relevant file. Passing a folderpath with throw a TypeError. Will not throw an
        error if the file exists - it will be overwritten
        :param content: str; The content to be written to the file as a string. Will throw a TypeError if it is not a
        string
        :return: None
        """
        pass

    @abc.abstractmethod
    def delete_file(self, filepath: str) -> None:
        """
        Delete file at a given filepath.
        :param filepath: str; Path to the relevant file. Passing a folderpath with throw a TypeError. Will not throw an
        error if the file does not exist.
        :return: None
        """
        pass


class LocalFilesystem(BaseFilesystem):
    # TODO: filelib wants to read files before finding suffixes etc. This means it can't operate on theoretical
    #  files. Should replace this with logic that can.
    @staticmethod
    def _get_path(path: str) -> Path:
        # Convert an incoming string into a Path object
        return Path(path)

    @staticmethod
    def _throw_error_if_non_existent(path: Path) -> None:
        if not path.exists():
            exc = KeyError(
                f"The given path does not exist. Inferred absolute path: '{path.absolute()}'."
            )
            log.warning(exc)
            raise exc

    @staticmethod
    def _throw_error_if_not_directory(path: Path) -> None:
        if not path.is_dir():
            exc = TypeError(
                f"The given path is not a directory. Inferred absolute path: '{path.absolute()}'."
            )
            log.warning(exc)
            raise exc

    @staticmethod
    def _throw_error_if_not_file(path: Path) -> None:
        if not path.is_file():
            exc = TypeError(
                f"The given path is not a file. Inferred absolute path: '{path.absolute()}'."
            )
            log.warning(exc)
            raise exc

    @staticmethod
    def _manual_is_file(path: str) -> bool:
        # pathlib wants to check local disk to figure out if something is a file, so need this to check if a
        # path is a file where it doesn't exist yet
        # True if it is a file, false otherwise
        return path[-1] != "/" and path[-1] != "\\"

    @staticmethod
    def _create_directories_as_needed(path: str) -> None:
        # Create any directories needed to fill out the path
        log.debug(f"Creating directories needed for path '{path}'.")
        os.makedirs(name=path, exist_ok=True)

    def posix_path(self, path: str) -> str:
        log.debug(f"Converting path {path} to posix form.")
        posix_path = Path(path).as_posix()
        return posix_path

    def list_files(self, folderpath: str) -> list[str]:
        log.debug(f"Listing files in path '{folderpath}'.")
        converted_path = self._get_path(folderpath)
        self._throw_error_if_non_existent(converted_path)
        self._throw_error_if_not_directory(converted_path)
        files = [
            str(item.name) for item in converted_path.iterdir() if not item.is_dir()
        ]
        log.debug(f"Found files: '{files}'.")
        return files

    def list_folders(self, folderpath: str) -> list[str]:
        log.debug(f"Listing folders in path '{folderpath}'.")
        converted_path = self._get_path(folderpath)
        self._throw_error_if_non_existent(converted_path)
        self._throw_error_if_not_directory(converted_path)
        folders = [
            f"{str(item.parts[-1])}/"
            for item in converted_path.iterdir()
            if item.is_dir()
        ]
        folders = list(filter(lambda a: a is not None, folders))
        log.debug(f"Found folders: '{folders}'.")
        return folders

    def list_contents(self, folderpath: str) -> list[str]:
        log.debug(f"Listing contents in path '{folderpath}'.")
        folders = self.list_folders(folderpath)
        files = self.list_files(folderpath)
        contents = folders + files
        log.debug(f"Found contents: '{contents}'.")
        return contents

    def path_exists(self, path: str) -> bool:
        log.debug(f"Checking if path exists: '{path}'.")
        converted_path = self._get_path(path)
        result = converted_path.exists()
        log.debug(f"Exist check result: '{result}'.")
        return result

    def read_file(self, filepath: str) -> str:
        log.info(f"Reading file at path '{filepath}'.")
        converted_path = self._get_path(filepath)
        self._throw_error_if_non_existent(converted_path)
        self._throw_error_if_not_file(converted_path)
        with open(converted_path) as file:
            content = file.read()
        file.close()
        log.debug(f"Size of content read: '{len(content)}'.")
        return content

    def is_directory(self, path: str) -> bool:
        log.debug(f"Checking if the item at path '{path}' is a directory.")
        converted_path = self._get_path(path)
        self._throw_error_if_non_existent(converted_path)
        result = converted_path.is_dir()
        log.debug(f"Directory check result: '{result}'.")
        return result

    def absolute_path(self, path: str) -> str:
        log.debug(f"Getting absolute path for path '{path}'.")
        converted_path = self._get_path(path)
        abs_path = converted_path.absolute().as_posix()
        log.debug(f"Absolute path is '{abs_path}'.")
        return abs_path

    def path_parent(self, path: str) -> str:
        log.debug(f"Getting parent for path '{path}'.")
        converted_path = self._get_path(path)
        parent_path = converted_path.parent.absolute()
        str_parent_path = parent_path.as_posix()
        log.debug(f"Parent is '{str_parent_path}'.")
        return str_parent_path

    def filename(self, filepath: str) -> str:
        log.debug(f"Getting filename for path '{filepath}'.")
        converted_path = self._get_path(filepath)
        if not self._manual_is_file(filepath):
            exc = TypeError(
                f"The given path is not a file. Inferred absolute path: '{converted_path.absolute()}'."
            )
            log.warning(exc)
            raise exc
        filename = converted_path.name
        log.debug(f"Filename is '{filename}'.")
        return filename

    def filename_stem(self, filepath: str) -> str:
        log.debug(f"Getting filename stem for path '{filepath}'.")
        converted_path = self._get_path(filepath)
        if not self._manual_is_file(filepath):
            exc = TypeError(
                f"The given path is not a file. Inferred absolute path: '{converted_path.absolute()}'."
            )
            log.warning(exc)
            raise exc
        stem = converted_path.stem
        raw_suffixes = converted_path.suffixes
        if not (len(raw_suffixes) == 0 or len(raw_suffixes)) == 1:
            suffixes = [str(suffix) for suffix in raw_suffixes]
            for i in range(2, len(suffixes) + 1):
                stem = stem.removesuffix(suffixes[-i])
        log.debug(f"Filename stem is '{stem}'.")
        return stem

    def filename_extension(self, filepath: str) -> str:
        log.debug(f"Getting filename extension for path '{filepath}'.")
        converted_path = self._get_path(filepath)
        if not self._manual_is_file(filepath):
            exc = TypeError(
                f"The given path is not a file. Inferred absolute path: '{converted_path.absolute()}'."
            )
            log.warning(exc)
            raise exc
        suffixes = converted_path.suffixes
        if len(suffixes) == 0:
            result = ""
        else:
            result = "".join(suffixes)
        log.debug(f"Filename extension is '{result}'.")
        return result

    def write_file(self, filepath: str, content: str) -> None:
        log.debug(f"Writing file content to path '{filepath}'.")
        converted_path = self._get_path(filepath)
        if not self._manual_is_file(path=filepath):
            exc = TypeError(
                f"The given path is not a filepath. Inferred absolute path: '{converted_path.absolute()}'."
            )
            log.warning(exc)
            raise exc
        parent_path = self.path_parent(path=filepath)
        self._create_directories_as_needed(path=parent_path)
        with open(file=converted_path, mode="w") as open_file:
            open_file.write(content)
        open_file.close()
        return None

    def delete_file(self, filepath: str) -> None:
        log.debug(f"Deleting file at path '{filepath}'.")
        converted_path = self._get_path(filepath)
        if not self._manual_is_file(path=filepath):
            exc = TypeError(
                f"The given path is not a filepath. Inferred absolute path: '{converted_path.absolute()}'."
            )
            log.warning(exc)
            raise exc
        os.remove(converted_path)
        return
