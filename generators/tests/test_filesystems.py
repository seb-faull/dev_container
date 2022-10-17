from src.adapters.filesystems import LocalFilesystem
import pytest
import os
from src.logger import log


@pytest.mark.integration_tests
class TestLocalFilesystem:

    asset_location = "tests/assets/local_filesystem"
    make_asset_location = "tests/assets/local_filesystem_make"

    def test_list_files(self, local_filesystem: LocalFilesystem):
        assert local_filesystem.list_files(f"{self.asset_location}/") == ["test.sql"]
        assert local_filesystem.list_files(f"{self.asset_location}/dir_a/") == [
            "bar.foo",
            "foo.bar",
        ]
        assert local_filesystem.list_files(f"{self.asset_location}/dir_b/") == [
            ".dotfile",
            "file.with.complex.type",
        ]

        with pytest.raises(TypeError):
            local_filesystem.list_files(f"{self.asset_location}/dir_b/.dotfile")

        with pytest.raises(KeyError):
            local_filesystem.list_files(
                f"{self.asset_location}/some_path_that_doesnt_exist/"
            )

    def test_list_folders(self, local_filesystem: LocalFilesystem):
        assert local_filesystem.list_folders(f"{self.asset_location}/") == [
            "dir_a/",
            "dir_b/",
        ]
        assert local_filesystem.list_folders(f"{self.asset_location}/dir_a/") == []

        with pytest.raises(TypeError):
            local_filesystem.list_folders(f"{self.asset_location}/dir_b/.dotfile")

        with pytest.raises(KeyError):
            local_filesystem.list_folders(
                f"{self.asset_location}/some_path_that_doesnt_exist/"
            )

    def test_list_contents(self, local_filesystem: LocalFilesystem):
        assert local_filesystem.list_contents(f"{self.asset_location}/") == [
            "dir_a/",
            "dir_b/",
            "test.sql",
        ]

        with pytest.raises(TypeError):
            local_filesystem.list_contents(f"{self.asset_location}/dir_b/.dotfile")

        with pytest.raises(KeyError):
            local_filesystem.list_contents(
                f"{self.asset_location}/some_path_that_doesnt_exist/"
            )

    def test_path_exists(self, local_filesystem: LocalFilesystem):
        assert local_filesystem.path_exists(self.asset_location) is True
        assert (
            local_filesystem.path_exists(
                f"{self.asset_location}/some_path_that_doesnt_exist/"
            )
            is False
        )

    def test_read_file(self, local_filesystem: LocalFilesystem):
        file_content = "SELECT * FROM SOME_TABLE;\nDELETE FROM SOME_TABLE;"
        assert (
            local_filesystem.read_file(f"{self.asset_location}/test.sql")
            == file_content
        )

        with pytest.raises(TypeError):
            local_filesystem.read_file(f"{self.asset_location}/dir_b/")

        with pytest.raises(KeyError):
            local_filesystem.read_file(
                f"{self.asset_location}/some_path_that_doesnt_exist/"
            )

    def test_is_directory(self, local_filesystem: LocalFilesystem):
        assert local_filesystem.is_directory(f"{self.asset_location}/") is True
        assert local_filesystem.is_directory(f"{self.asset_location}/test.sql") is False

        with pytest.raises(KeyError):
            local_filesystem.is_directory(
                f"{self.asset_location}/some_path_that_doesnt_exist/"
            )

    def test_absolute_path(self, local_filesystem: LocalFilesystem):
        abs_path = local_filesystem.posix_path(
            os.path.abspath(f"{self.asset_location}/test.sql")
        )
        assert (
            local_filesystem.absolute_path(f"{self.asset_location}/test.sql")
            == abs_path
        )
        log.info(f"Calculated absolute path is {abs_path}")

    def test_filename(self, local_filesystem: LocalFilesystem):
        assert (
            local_filesystem.filename(f"{self.asset_location}/dir_b/.dotfile")
            == ".dotfile"
        )
        assert (
            local_filesystem.filename(f"{self.asset_location}/test.sql") == "test.sql"
        )
        assert (
            local_filesystem.filename(
                f"{self.asset_location}/dir_b/file.with.complex.type"
            )
            == "file.with.complex.type"
        )

        with pytest.raises(TypeError):
            local_filesystem.filename(f"{self.asset_location}/dir_b/")

    def test_filename_stem(self, local_filesystem: LocalFilesystem):
        assert (
            local_filesystem.filename_stem(f"{self.asset_location}/dir_b/.dotfile")
            == ".dotfile"
        )
        assert (
            local_filesystem.filename_stem(f"{self.asset_location}/test.sql") == "test"
        )
        assert (
            local_filesystem.filename_stem(
                f"{self.asset_location}/dir_b/file.with.complex.type"
            )
            == "file"
        )

        with pytest.raises(TypeError):
            local_filesystem.filename_stem(f"{self.asset_location}/dir_b/")

    def test_filename_extension(self, local_filesystem: LocalFilesystem):
        assert (
            local_filesystem.filename_extension(f"{self.asset_location}/dir_b/.dotfile")
            == ""
        )
        assert (
            local_filesystem.filename_extension(f"{self.asset_location}/test.sql")
            == ".sql"
        )
        assert (
            local_filesystem.filename_extension(
                f"{self.asset_location}/dir_b/file.with.complex.type"
            )
            == ".with.complex.type"
        )

        with pytest.raises(TypeError):
            local_filesystem.filename_extension(f"{self.asset_location}/dir_b/")

    @pytest.mark.parametrize(
        "path, expected_result",
        [
            ("test.sql", ""),
            ("dir_a/bar.foo", "/dir_a/"),
            ("dir_b/.dotfile", "dir_b/"),
        ],
    )
    def test_path_parent(self, local_filesystem, path, expected_result):
        full_path = f"{self.asset_location}/{path}"
        expected_result = local_filesystem.posix_path(
            os.path.abspath(f"{self.asset_location}/{expected_result}")
        )
        assert local_filesystem.path_parent(path=full_path) == expected_result

    @pytest.mark.parametrize(
        "path, expected_result",
        [
            ("./test.sql", "test.sql"),
            ("test.sql", "test.sql"),
            (".dotfile", ".dotfile"),
            ("./.dotfile", ".dotfile"),
            ("dir_a/bar.foo", "dir_a/bar.foo"),
            ("", "."),
        ],
    )
    def test_as_posix(self, local_filesystem, path, expected_result):
        assert local_filesystem.posix_path(path) == expected_result

    def test_write_file(self, local_filesystem):
        full_path = f"{self.make_asset_location}/delete.me"
        if local_filesystem.path_exists(path=full_path):
            local_filesystem.delete_file(filepath=full_path)
        assert local_filesystem.path_exists(path=full_path) is False
        local_filesystem.write_file(filepath=full_path, content="test information")
        assert local_filesystem.path_exists(path=full_path) is True
        assert local_filesystem.read_file(filepath=full_path) == "test information"
        local_filesystem.delete_file(filepath=full_path)
        if local_filesystem.path_exists(path=full_path) is True:
            log.error(
                "Failed to properly clean up test. Delete the file at '{full_path}'."
            )

    def test_delete_file(self, local_filesystem):
        full_path = f"{self.make_asset_location}/make.me"
        if not local_filesystem.path_exists(path=full_path):
            local_filesystem.write_file(filepath=full_path, content="")
        assert local_filesystem.path_exists(path=full_path) is True
        local_filesystem.delete_file(filepath=full_path)
        assert local_filesystem.path_exists(path=full_path) is False
        local_filesystem.write_file(filepath=full_path, content="")
        if not local_filesystem.path_exists(path=full_path):
            log.error(
                "Failed to properly clean up test. Create an empty file at '{full_path}'."
            )
