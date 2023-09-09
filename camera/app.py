from routes.model import create_app
import uvicorn


app = create_app()

if __name__ == "__main__":
    uvicorn.run(
        "app:app", host="0.0.0.0", port=9080, 
        log_level="info", reload=True
    )
