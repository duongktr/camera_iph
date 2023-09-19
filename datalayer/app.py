from typing import *
from fastapi import FastAPI
from fastapi.openapi.docs import (
    get_redoc_html,
    get_swagger_ui_html,
    get_swagger_ui_oauth2_redirect_html,
)
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from backend.es import ElasticsearchBackend
from backend.kafka_custom import KafkaReader, KafkaSender
from backend.milvus import MilvusBackend

from loguru import logger

elasticsearch = ElasticsearchBackend(host="0.0.0.0", port="9200")
milvus = MilvusBackend(host="0.0.0.0", port="19530", collection="iph_feature_data")


def create_backend_app():

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
    
    @app.get("/api/server")
    async def get_data():
        try:
            a=5
        except Exception as e:
            logger.info(e)

            return JSONResponse(status_code=404)
        
        return JSONResponse(content="Get data success", 
                            status_code=200)
    @app.post("/api/server")
    async def send_match_result():
        try:
            """
                check if
                logic about query similarity vector
            """
        except Exception as e:
            logger.info(e)

            return JSONResponse(status_code=401)
        return JSONResponse(content="Result about matching or not",status_code=201)
    
    @app.put("/api/server")
    async def update_data(data, id):
        """
            Logic update data
        """
        return
    
    @app.delete("/api/server")
    async def delete_data(id):
        try:
            """logic check id if exist or not, then delete"""
            a=5
        except Exception as e:
            logger.info(e)
            return JSONResponse(status_code=400)
        return JSONResponse(content="Delete success"
                            ,status_code=200)

    return app