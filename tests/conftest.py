import pytest
from src.adapters.filesystems import LocalFilesystem
from tests.mocks.mock_filesystem import PatchedLocalFilesystem


@pytest.fixture()
def local_filesystem() -> LocalFilesystem:
    return LocalFilesystem()


@pytest.fixture()
def patched_local_filesystem() -> PatchedLocalFilesystem:
    return PatchedLocalFilesystem()
