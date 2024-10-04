import random
import shutil
import sqlite3
import time
import httpx
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Metric, Dimension, OrderBy, RunReportRequest
from google.oauth2 import service_account
from google.analytics.data import BetaAnalyticsDataClient
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from start_files.models.mls.rentals_db_section import get_rentals_from_db
from start_files.models.mls.homes_db_section import get_homes_from_db
from start_files.routes.rentals_scripts import sorted_rentals_by_price, start_rentals
from start_files.routes.homes_scripts import sorted_homes_by_price, start_homes
from fastapi import APIRouter, Form, Path, Request, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from google.analytics.data import BetaAnalyticsDataClient
from google.oauth2 import service_account
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from dotenv import load_dotenv
from sqlalchemy import func
from typing import List
from PIL import Image
import logging
import httpx
import os

router = APIRouter()

load_dotenv()
SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")
RECIPIENT = os.getenv("RECIPIENT")
SENDER = os.getenv("SENDER")
MAILJET_API = os.getenv("MAILJET_API")
MAILJET_SECRET = os.getenv("MAILJET_SECRET")
GA_VIEW_ID = os.getenv("GA_VIEW_ID")


# Set up logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)


# Home route
@router.get("/", response_class=HTMLResponse, name="home")
async def read_root(request: Request):
    logger.info("Rendering home page")
    templates = request.app.state.templates
    return templates.TemplateResponse("home.html", {"request": request})


@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
# Load Google Analytics credentials
    credentials = service_account.Credentials.from_service_account_file('services/yonehomes-50e3014c6fd5.json')
    client = BetaAnalyticsDataClient(credentials=credentials)
    property_id = '461503351'
    templates = request.app.state.templates
    
    # Fetch analytics data
    total_users = fetch_total_users(client,property_id)  
    sessions =  fetch_sessions(client,property_id)
    bounce_rate =  fetch_bounce_rate(client,property_id)
    avg_session_duration =  fetch_avg_session_duration(client,property_id)
    new_users =  fetch_new_users(client,property_id)
    conversions =  fetch_conversions(client,property_id)
    real_time_users = fetch_real_time_users(client, property_id)

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "total_users": total_users,
        "sessions": sessions,
        "bounce_rate": bounce_rate,
        "avg_session_duration": avg_session_duration,
        "new_users": new_users,
        "conversions": conversions,
        "real_time_users": real_time_users,
    })

def fetch_total_users(client,property_id):
    request = {
        'property': f"properties/{property_id}",
        'date_ranges': [{'start_date': '2023-01-01', 'end_date': '2023-01-31'}],
        'dimensions': [{'name': 'city'}],
        'metrics': [{'name': 'activeUsers'}],
    }
    response = client.run_report(request)  # No await here
    
    if not response.rows:
        return 0  # Default value if no data

    return int(response.rows[0].metric_values[0].value)


def fetch_sessions(client,property_id):
    request = {
        'property': f'properties/{property_id}',
        'date_ranges': [{"start_date": "30daysAgo", "end_date": "today"}],
        'metrics': [{"name": "sessions"}]
    }
    response = client.run_report(request)  # No await
    
    if not response.rows:
        return 0  # Default value if no data
    
    return int(response.rows[0].metric_values[0].value)

def fetch_bounce_rate(client,property_id):
    request = {
        'property': f'properties/{property_id}',
        'date_ranges': [{"start_date": "30daysAgo", "end_date": "today"}],
        'metrics': [{"name": "bounceRate"}]
    }
    response = client.run_report(request)  # No await
    
    if not response.rows:
        return 0.0  # Default value if no data
    
    return float(response.rows[0].metric_values[0].value)

def fetch_avg_session_duration(client,property_id):
    request = {
        'property': f'properties/{property_id}',
        'date_ranges': [{"start_date": "30daysAgo", "end_date": "today"}],
        'metrics': [{"name": "averageSessionDuration"}]
    }
    response = client.run_report(request)  # No await
    
    if not response.rows:
        return 0.0  # Default value if no data
    
    return float(response.rows[0].metric_values[0].value)

def fetch_new_users(client,property_id):
    request = {
        'property': f'properties/{property_id}',
        'date_ranges': [{"start_date": "30daysAgo", "end_date": "today"}],
        'metrics': [{"name": "newUsers"}]
    }
    response = client.run_report(request)  # No await
    
    if not response.rows:
        return 0  # Default value if no data
    
    return int(response.rows[0].metric_values[0].value)


def fetch_conversions(client,property_id):
    request = {
        'property': f'properties/{property_id}',
        'date_ranges': [{"start_date": "30daysAgo", "end_date": "today"}],
        'metrics': [{"name": "conversions"}]
    }
    response = client.run_report(request)  # No await
    
    if not response.rows:
        return 0  # Default value if no data
    
    return int(response.rows[0].metric_values[0].value)


def fetch_real_time_users(client, property_id):
    # Define the request parameters
    request = RunReportRequest(
        property=f'properties/{property_id}',
        date_ranges=[DateRange(start_date="today", end_date="today")],  # Adjust as necessary
        metrics=[Metric(name="activeUsers")],
        dimensions=[Dimension(name="pageTitle"), Dimension(name="pagePath")],
        order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="activeUsers"))]
    )

    try:
        response = client.run_report(request)  # Send the request to the API
        print("Response received:", response)  # Debugging line

        result = []
        if response.rows:  # Check if there are rows in the response
            for row in response.rows:
                result.append({
                    "page_title": row.dimension_values[0].value,
                    "page_path": row.dimension_values[1].value,
                    "active_users": int(row.metric_values[0].value)
                })
        else:
            print("No data returned for the specified date range and metrics.")  # Debugging line

        return result  # Return the results

    except Exception as e:
        print("Error fetching data:", e)  # Error handling
        return []
    
@router.get("/rentals", response_class=HTMLResponse, name="rentals")
async def rentals(request: Request):
    logger.info("Rendering rentals page")
    templates = request.app.state.templates
    return templates.TemplateResponse("rentals.html", {"request": request})

@router.get("/buying", response_class=HTMLResponse, name="buying")
async def buying(request: Request):
    logger.info("Rendering buying page")
    templates = request.app.state.templates
    return templates.TemplateResponse("buying.html", {"request": request})

@router.get("/resources", response_class=HTMLResponse, name="resources")
async def resources(request: Request):
    logger.info("Rendering resources page")
    templates = request.app.state.templates
    return templates.TemplateResponse("resources.html", {"request": request})

@router.get("/contact", response_class=HTMLResponse, name="contact")
async def contact(request: Request):
    logger.info("Rendering contact page")
    templates = request.app.state.templates
    return templates.TemplateResponse("contact.html", {"request": request})



@router.post("/contact")
async def handle_contact_form(
    request: Request,
    first_name: str = Form(...),
    last_name: str = Form(...),
    email: str = Form(...),
    phone: str = Form(None),
    general_inquiry: str = Form(...)
):
    logger.info(f"Received contact form submission from {email}")
    
    data = {
        'FromEmail': SENDER,
        'FromName': email,  # Replace with your name or company name
        'Subject': 'New Contact Form Submission',
        'Text-part': 'Hey Toni, here is another support lead!',
        'Html-part': f'''
            <p>First Name: {first_name}</p>
            <p>Last Name: {last_name}</p>
            <p>Email: {email}</p>
            <p>Phone: {phone}</p>
            <p>General Inquiry: {general_inquiry}</p>
        ''',        
        'Recipients': [{ "Email": RECIPIENT }]
    }

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                'https://api.mailjet.com/v3/send',
                json=data,
                auth=(MAILJET_API, MAILJET_SECRET)
            )
        
        logger.info(f"Mailjet response status code: {response.status_code}")

        if response.status_code != 200:
            logger.error(f"Mailjet error: {response.text}")
            raise HTTPException(status_code=500, detail="Error sending email")
    except Exception as e:
        logger.error(f"Error sending email: {e}")
        raise HTTPException(status_code=500, detail="Error sending email")

    logger.info("Contact form submission successful, redirecting user.")
    return RedirectResponse(url="/contact", status_code=303)


@router.get("/api/filter_rentals_export", response_class=JSONResponse, name="filter")
async def sort_rental_endpoint(max_price: int = 2500):
    logger.info("Starting CSV sort task in the background")
    sorted = sorted_rentals_by_price(max_price)
    return f"This sorted list has {len(sorted)} listings."

@router.get("/api/populate_rentals_database", response_model=dict, name="import")
async def get_rental_data(background_tasks: BackgroundTasks, concurrency_limit: int = 10,  max_retries: int = 20, delay: int= 1, timeout: int = 300, min_images: int = 2, max_images: int = 50, max_price: int =3000):
    logger.info("Starting MLS rentals gathering task")
    background_tasks.add_task(start_rentals, concurrency_limit, timeout, max_images, min_images, max_price, max_retries, delay)
    return JSONResponse(content={"message": "MLS data gathering rental task started in the background"})

@router.get('/api/view_rentals_database')
async def api_rentals():
    try:
        listings_data = get_rentals_from_db()
        return {"rentals":listings_data}
    except Exception:
        raise HTTPException(status_code=500)

@router.get('/api/rentals_database_count')
async def get_total_count():
    try:
        listings_data = get_rentals_from_db()
        return {"rentals Count":len(listings_data)}
    except Exception:
        raise HTTPException(status_code=500)

@router.get("/api/filter_homes_export", response_class=JSONResponse, name="filter")
async def sort_homes_endpoint(max_price: int = 2500):
    logger.info("Starting CSV sort task in the background")
    sorted = sorted_homes_by_price(max_price)
    return f"This sorted list has {len(sorted)} listings."

@router.get("/api/populate_homes_database", response_model=dict, name="import")
async def get_home_data(background_tasks: BackgroundTasks, concurrency_limit: int = 10,  max_retries: int = 20, delay: int= 1, timeout: int = 300, min_images: int = 2, max_images: int = 100, max_price: int = 500000):
    logger.info("Starting MLS homes gathering task")
    background_tasks.add_task(start_homes, concurrency_limit, timeout, max_images, min_images, max_price, max_retries, delay)
    return JSONResponse(content={"message": "MLS data gathering home task started in the background"})

@router.get('/api/view_homes_database')
async def api_homes():
    try:
        listings_data = get_homes_from_db()
        return {"homes":listings_data}
    except Exception:
        raise HTTPException(status_code=500)

@router.get('/api/homes_database_count')
async def get_total_count():
    try:
        listings_data = get_homes_from_db()
        return {"homes count":len(listings_data)}
    except Exception:
        raise HTTPException(status_code=500)
    
@router.get("/home_search")
async def home_search(request: Request):
    external_url = "https://otonielyone.unitedrealestatewashingtondc.com/index.htm"
    async with httpx.AsyncClient() as client:
        response = await client.get(external_url)
        return StreamingResponse(
            content=response.aiter_raw(),
            headers=dict(response.headers),
            status_code=response.status_code
        )


@router.get("convert_images_to_webp")
def convert_images_to_webp():
    base_folder = 'fastapi_project/start_files/static'  
    print("Current Working Directory:", os.getcwd())
    for folder in os.listdir(base_folder):
        folder_path = os.path.join(base_folder, folder)
        # Check if it's a directory
        if os.path.isdir(folder_path):
            for root, dirs, files in os.walk(folder_path):
                for file in files:
                    if file.lower().endswith('.jpg'):
                        jpg_path = os.path.join(root, file)
                        webp_path = os.path.splitext(jpg_path)[0] + '.webp'
                        
                        # Open the JPEG image and convert it to WebP
                        with Image.open(jpg_path) as img:
                            img.save(webp_path, 'WEBP')
                            print(f'Converted {jpg_path} to {webp_path}')


@router.get("restore_backups")
def restore_backups():
    db_path = "brightscrape/brightmls_rentals.db.bak"
    new_db_name = db_path[:-4]

    if os.path.exists(db_path):
        os.rename(db_path, new_db_name)

    base_dir = '/var/www/html/fastapi_project/start_files/static/rentals_images/'

    for item in os.listdir(base_dir):
        item_path = os.path.join(base_dir, item)

        if os.path.isdir(item_path) and item.endswith('.bak'):
            new_name = item[:-4]
            new_path = os.path.join(base_dir, new_name)
            try:
                shutil.move(item_path, new_path)
                print(f"Renamed directory: {item_path} to {new_path}")
            except Exception:
                print(f"Error renaming directory {item_path} to {new_path}")
        
        elif os.path.isdir(item_path):
            try:
                shutil.rmtree(item_path)
                print(f"Removed non-backup directory: {item_path}")
            except Exception:
                print(f"Error removing directory {item_path}")