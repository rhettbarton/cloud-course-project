import os
from datetime import datetime
from re import A
from typing import (
    List,
    Optional,
)

from fastapi import (
    APIRouter,
    Depends,
    FastAPI,
    Response,
    UploadFile,
    status,
)
from fastapi.responses import StreamingResponse
from flask import request
from pydantic import BaseModel

from files_api.s3.delete_objects import delete_s3_object
from files_api.s3.read_objects import (
    fetch_s3_object,
    fetch_s3_objects_metadata,
    fetch_s3_objects_using_page_token,
    object_exists_in_s3,
)
from files_api.s3.write_objects import upload_s3_object

####################################
# --- Request/response schemas --- #
####################################


# read (cRud)
class FileMetadata(BaseModel):
    file_path: str
    last_modified: datetime
    size_bytes: int


class PutFileResponse(BaseModel):
    file_path: str
    message: str


class GetFilesResponse(BaseModel):
    files: List[FileMetadata]
    next_page_token: Optional[str]


# read (cRud)
class GetFilesQueryParams(BaseModel):
    page_size: int = 10
    directory: Optional[str] = ""
    page_token: Optional[str] = None
