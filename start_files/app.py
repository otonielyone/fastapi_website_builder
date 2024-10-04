from google.analytics.data import BetaAnalyticsDataClient
from start_files.config import get_templates
from start_files.routes.routes import router
from fastapi.staticfiles import StaticFiles
from google.oauth2 import service_account
from fastapi import FastAPI
import logging

def create_app() -> FastAPI:
    app = FastAPI()
    app.mount("/static", StaticFiles(directory="start_files/static"), name="static")
    app.include_router(router)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("logs/yonehomes.log")
        ]
    )
    
    templates = get_templates()
    app.state.templates = templates
    
    return app