from typing import *
from fastapi import FastAPI
from fastapi.openapi.docs import (
    get_redoc_html,
    get_swagger_ui_html,
    get_swagger_ui_oauth2_redirect_html,
)
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from loguru import logger
import json

from utility.controller import PController
from utility.capture import read_frame

capture = PController.get_instance()


class DataModel(BaseModel):
    camera: List[str] = [
        "/home/hoang/Documents/alpr/tests/Video_Camera_KMF/Scenario_1/192.168.10.55.avi",
    ]


def create_app():
    config_path = "streams.json"
    app = FastAPI(docs_url=None, redoc_url=None)
    app.mount("/static", StaticFiles(directory="static"), name="static")

    @app.get("/docs", include_in_schema=False)
    async def custom_swagger_ui_html():
        return get_swagger_ui_html(
            openapi_url=app.openapi_url,
            title=app.title + " - Swagger UI",
            oauth2_redirect_url=app.swagger_ui_oauth2_redirect_url,
            swagger_js_url="/static/swagger-ui-bundle.js",
            swagger_css_url="/static/swagger-ui.css",
        )

    @app.get(app.swagger_ui_oauth2_redirect_url, include_in_schema=False)
    async def swagger_ui_redirect():
        return get_swagger_ui_oauth2_redirect_html()

    @app.get("/redoc", include_in_schema=False)
    async def redoc_html():
        return get_redoc_html(
            openapi_url=app.openapi_url,
            title=app.title + " - ReDoc",
            redoc_js_url="/static/redoc.standalone.js",
        )

    @app.get("/api/camera")
    async def get_camera():
        try:
            with open(config_path) as f:
                sources = json.load(f)
        except Exception as e:
            sources = {}
        sources: list = sources.get("camera", [])
        content = {"camera": sources}

        logger.info("Running process: {}".format(capture.processes))

        return JSONResponse(
            content=content, status_code=200
        )

    @app.post("/api/camera")
    async def add_camera(data: DataModel):
        # Load sources
        try:
            with open(config_path) as f:
                sources = json.load(f)
        except Exception as e:
            sources = {}
        sources: list = sources.get("camera", [])

        # Add sources
        sources.extend(data.camera)
        sources = list(set(sources))
        content = {"camera": sources}
        with open(config_path, "w") as f:
            json.dump(content, f, indent=2)

        for source in sources:
            if source in capture.processes:  # running process
                continue
            capture.add_process(source, read_frame, (source,))

        logger.info("Running process: {}".format(capture.processes))

        return JSONResponse(
            content=content, status_code=200
        )

    @app.delete("/api/camera")
    async def delete_camera(data: DataModel):
        # Load sources
        try:
            with open(config_path) as f:
                sources = json.load(f)
        except Exception as e:
            sources = {}
        sources: list = sources.get("camera", [])

        for source in sources:
            if source not in data.camera:  # running process
                continue
            capture.kill_process(source, force=True)

        # Update sources
        sources = [source for source in sources if source not in data.camera]
        content = {"camera": sources}
        with open(config_path, "w") as f:
            json.dump(content, f, indent=2)

        logger.info("Running process: {}".format(capture.processes))

        return JSONResponse(
            content=content, status_code=200
        )

    return app
