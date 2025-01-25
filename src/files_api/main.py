import os

from fastapi import FastAPI
from flask import request

from files_api.routes import ROUTER


def create_app(s3_bucket_name: str | None = None) -> FastAPI:
    """Create a FastAPI application"""
    app = FastAPI()
    s3_bucket_name = s3_bucket_name or os.environ["S3_BUCKET_NAME"]
    request.state.s3_bucket_name = s3_bucket_name
    app.include_router(ROUTER)

    return app


if __name__ == "__main__":
    import uvicorn

    app = create_app()
    uvicorn.run(app, host="0.0.0.0", port=8000)
