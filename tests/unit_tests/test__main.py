import re
import stat
from calendar import c
from wsgiref import headers

import pytest
from fastapi import status
from fastapi.testclient import TestClient

from src.files_api.main import APP


# Fixture for FastAPI test client
@pytest.fixture
def client(mocked_aws: None) -> TestClient:  # pylint: disable=unused-argument
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


def test__list_files_with_pagination(client: TestClient):
    # create a few files
    file_paths = [
        "file1.txt",
        "file2.txt",
        "file3.txt",
        "subfolder/file4.txt",
        "subfolder/file5.txt",
    ]
    for file_path in file_paths:
        client.put(
            f"/files/{file_path}",
            files={"file": (file_path, b"test file contents", "text/plain")},
        )

    # list files with default page size
    response = client.get("/files")
    assert response.status_code == status.HTTP_200_OK
    assert len(response.json()["files"]) == 5
    assert response.json()["next_page_token"] is None

    # list files with page size 2
    response = client.get("/files?page_size=2")
    assert response.status_code == status.HTTP_200_OK
    assert len(response.json()["files"]) == 2
    assert response.json()["next_page_token"] is not None

    # list files with page size 2 and page token
    response = client.get(f"/files?page_size=2&page_token={response.json()['next_page_token']}")
    assert response.status_code == status.HTTP_200_OK
    assert len(response.json()["files"]) == 2
    assert response.json()["next_page_token"] is not None

    # list files with directory and page size 1
    response = client.get("/files?directory=subfolder&page_size=1")
    assert response.status_code == status.HTTP_200_OK
    assert len(response.json()["files"]) == 1
    assert response.json()["next_page_token"] is not None


def test_get_file_metadata(client: TestClient):
    # create a file
    test_file_path = "some/nested/file.txt"
    test_file_contents = b"test file contents"
    test_file_content_type = "text/plain"

    client.put(
        f"/files/{test_file_path}",
        files={"file": (test_file_path, test_file_contents, test_file_content_type)},
    )

    response = client.head(f"/files/{test_file_path}")

    assert response.headers["Content-Type"] == test_file_content_type
    assert response.headers["Content-Length"] == str(len(test_file_contents))
    assert response.headers["Last-Modified"] is not None
    assert response.status_code == status.HTTP_200_OK


def test_get_file(client: TestClient): ...


def test_delete_file(client: TestClient): ...
