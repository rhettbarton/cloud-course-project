import stat
from wsgiref import headers

import pytest
from fastapi import status
from fastapi.testclient import TestClient

from src.files_api.main import APP


# Fixture for FastAPI test client
@pytest.fixture
def client(mocked_aws) -> TestClient:  # pylint: disable=unused-argument
    with TestClient(APP) as client:
        yield client


def test__upload_file__happy_path(client: TestClient):
    # create a file
    test_file_path = "some/nested/file.txt"
    test_file_contents = b"test file contents"
    test_file_content_type = "text/plain"

    response = client.put(
        f"/files/{test_file_path}",
        files={"file": (test_file_path, test_file_contents, test_file_content_type)},
    )

    assert response.status_code == status.HTTP_201_CREATED
    assert response.json() == {"file_path": test_file_path, "message": f"New file uploaded at path: {test_file_path}"}

    # upload the file
    updated_contents = b"updated file contents"
    response = client.put(
        f"/files/{test_file_path}",
        files={"file": (test_file_path, updated_contents, test_file_content_type)},
    )

    assert response.status_code == status.HTTP_200_OK
    assert response.json() == {
        "file_path": test_file_path,
        "message": f"Exisiting file update at path: {test_file_path}",
    }


def test_list_files_with_pagination(client: TestClient): ...


def test_get_file_metadata(client: TestClient): ...


def test_get_file(client: TestClient): ...


def test_delete_file(client: TestClient): ...
