
from start_files.models.mls.mls import init_db
from start_files.config import get_templates
from start_files.routes.routes import router
from fastapi.staticfiles import StaticFiles
from fastapi import FastAPI
import logging

def create_app() -> FastAPI:
    app = FastAPI()
    app.mount("/static", StaticFiles(directory="start_files/static"), name="static")
    app.include_router(router)
    init_db()

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("/var/www/html/fastapi_project/logs/app.log")
        ]
    )
    
    templates = get_templates()
    app.state.templates = templates
    
    return app
