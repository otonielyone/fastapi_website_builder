from start_files.models.mls.analytics import Analytics, analytics_sessionLocal, get_analytics_from_db
from google.analytics.data_v1beta.types import DateRange, Metric, Dimension, RunReportRequest
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from start_files.models.mls.rentals_db_section import get_rentals_from_db
from start_files.models.mls.homes_db_section import get_homes_from_db
from start_files.routes.rentals_scripts import sorted_rentals_by_price, start_rentals
from start_files.routes.homes_scripts import sorted_homes_by_price, start_homes
from fastapi import APIRouter, Form, Path, Request, HTTPException, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from google.oauth2 import service_account
from datetime import datetime, timedelta
from sqlalchemy import desc, func
from typing import Any, Dict, List
from collections import Counter
from dotenv import load_dotenv
from PIL import Image
import logging
import shutil
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


@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    try:
        # Authentication
        credentials = service_account.Credentials.from_service_account_file('services/yonehomes-50e3014c6fd5.json')
        client = BetaAnalyticsDataClient(credentials=credentials)
        property_id = '461503351'

        # Date range
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=30)

        # GA request
        ga_request = RunReportRequest(
            property=f"properties/{property_id}",
            date_ranges=[DateRange(start_date=start_date.strftime("%Y-%m-%d"), end_date=end_date.strftime("%Y-%m-%d"))],
            metrics=[
                Metric(name="activeUsers"),
                Metric(name="averageSessionDuration"),
                Metric(name="bounceRate"),
                Metric(name="newUsers"),
                Metric(name="screenPageViewsPerSession"),
                Metric(name="scrolledUsers"),
                Metric(name="sessions"),
                Metric(name="userEngagementDuration"),
            ],
            dimensions=[
                Dimension(name="browser"),
                Dimension(name="city"),
                Dimension(name="country"),
                Dimension(name="date"),  # Make sure this matches the right index
                Dimension(name="hostName"),
                Dimension(name="language"),
                Dimension(name="operatingSystem"),
                Dimension(name="platform"),
                Dimension(name="region"),    
            ]
        )

        # Fetch data
        response = client.run_report(ga_request)

        # Database processing
        db = analytics_sessionLocal()
        try:
            for row in response.rows:
                # Correctly extract the date from the row
                date = datetime.strptime(row.dimension_values[3].value, "%Y%m%d").date()  # Correct format if needed
                def safe_int(value):
                    try:
                        return int(float(value))  # Convert to float first and then to int
                    except ValueError:
                        return 0  # Or handle the error as needed

                analytics = Analytics(
                    active_users=safe_int(row.metric_values[0].value),  # Active Users
                    average_session_duration=float(row.metric_values[1].value),  # Average Session Duration
                    bounce_rate=float(row.metric_values[2].value),  # Bounce Rate
                    new_users=safe_int(row.metric_values[3].value),  # New Users
                    screen_page_views_per_session=float(row.metric_values[4].value),  # Screen Page Views Per Session
                    scrolled_users=safe_int(row.metric_values[5].value),  # Scrolled Users
                    sessions=safe_int(row.metric_values[6].value),  # Sessions
                    user_engagement_time=safe_int(row.metric_values[7].value),  # User Engagement Duration
                    date=date,
                    browser=row.dimension_values[0].value,  # Browser
                    city=row.dimension_values[1].value,  # City
                    country=row.dimension_values[2].value,  # Country
                    host_name=row.dimension_values[4].value,  # Host Name
                    language=row.dimension_values[5].value,  # Language
                    operating_system=row.dimension_values[6].value,  # Operating System
                    platform=row.dimension_values[7].value,  # Platform
                    region=row.dimension_values[8].value,  # Region
                )

                db.merge(analytics)
                db.commit()
        except Exception as e:
            logger.error(f"Database error: {str(e)}")
            db.rollback()
            raise HTTPException(status_code=500, detail="Database error occurred")
        finally:
            db.close()

        # Fetch calculated dashboard data
        db = analytics_sessionLocal()
        try:
            dashboard_data = get_dashboard_data(db)  # Ensure this function is defined elsewhere
        except Exception as e:
            logger.error(f"Error calculating dashboard data: {str(e)}")
            raise HTTPException(status_code=500, detail="Error calculating dashboard data")
        finally:
            db.close()

        templates = request.app.state.templates  # Ensure templates are set up in your app
        return templates.TemplateResponse("dashboard.html", {"request": request, "dashboard_data": dashboard_data})
    except Exception as e:
        error_message = str(e)
        
        # Log all other unexpected errors
        logger.error(f"Unexpected error in dashboard route: {error_message}")
        raise HTTPException(status_code=500, detail="An unexpected error occurred")


def get_dashboard_data(db_session, days: int = 30) -> Dict[str, Any]:
    """Retrieve and calculate all metrics for the dashboard."""
    analytics_data = get_analytics_data(db_session, days)
    
    return {
        "total_sessions": calculate_total_sessions(analytics_data),  # Total Sessions
        "total_users": calculate_total_users(analytics_data),  # Total Users
        "new_users": calculate_new_users(analytics_data),  # New Users
        "active_users": calculate_active_users(analytics_data),  # Active Users
        "user_engagement_duration": calculate_total_user_engagement_duration(analytics_data),  # User Engagement Duration
        "avg_session_duration": calculate_avg_session_duration(analytics_data),  # Average Session Duration
        "bounce_rate": calculate_bounce_rate(analytics_data),  # Bounce Rate
        "screen_page_views_per_session": calculate_avg_screens_per_session(analytics_data),  # Screen Page Views Per Session
        "scrolled_users": calculate_total_scrolled_users(analytics_data),  # Scrolled Users
        "browser": calculate_browser(analytics_data),  # Browser
        "city": calculate_city(analytics_data),  # City
        "country": calculate_country(analytics_data),  # Country
        "host_name": calculate_host_name(analytics_data),  # Host Name
        "language": calculate_language(analytics_data),  # Language
        "operating_system": calculate_operating_system(analytics_data),  # Operating System
        "platform": calculate_platform(analytics_data),  # Platform
        "region": calculate_region(analytics_data),  # Region
    }

def get_analytics_data(db_session, days: int = 30) -> List[Analytics]:
    """Retrieve analytics data for the specified number of days."""
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days)  # This uses timedelta correctly
    
    return db_session.query(Analytics).filter(
        Analytics.date >= start_date,
        Analytics.date <= end_date
    ).order_by(desc(Analytics.date)).all()

def calculate_total_sessions(analytics_data: List[Analytics]) -> int:
    """Calculate total sessions for the given period."""
    return sum(data.sessions for data in analytics_data)

def calculate_total_users(analytics_data: List[Analytics]) -> int:
    """Calculate total unique users for the given period."""
    return sum(data.new_users for data in analytics_data)

def calculate_new_users(analytics_data: List[Analytics]) -> int:
    """Calculate total new users for the given period."""
    return sum(data.new_users for data in analytics_data)

def calculate_active_users(analytics_data: List[Analytics]) -> int:
    """Calculate total active users for the given period."""
    return sum(data.active_users for data in analytics_data)

def calculate_total_user_engagement_duration(analytics_data: List[Analytics]) -> int:
    """Calculate total user engagement duration for the given period."""
    return sum(data.user_engagement_time for data in analytics_data)

def calculate_avg_session_duration(analytics_data: List[Analytics]) -> float:
    """Calculate average session duration for the given period."""
    total_duration = sum(data.average_session_duration for data in analytics_data)
    total_sessions = calculate_total_sessions(analytics_data)
    return total_duration / total_sessions if total_sessions > 0 else 0

def calculate_bounce_rate(analytics_data: List[Analytics]) -> float:
    """Calculate bounce rate for the given period."""
    total_sessions = calculate_total_sessions(analytics_data)
    bounced_sessions = sum(1 for data in analytics_data if data.bounce_rate < 100)  # Adjust as needed
    return (bounced_sessions / total_sessions) * 100 if total_sessions > 0 else 0

def calculate_avg_screens_per_session(analytics_data: List[Analytics]) -> float:
    """Calculate average screen page views per session."""
    total_screens = sum(data.screen_page_views_per_session for data in analytics_data)
    total_sessions = calculate_total_sessions(analytics_data)
    return total_screens / total_sessions if total_sessions > 0 else 0

def calculate_total_scrolled_users(analytics_data: List[Analytics]) -> int:
    """Calculate total scrolled users for the given period."""
    return sum(data.scrolled_users for data in analytics_data)

def calculate_host_name(analytics_data: List[Analytics]) -> str:
    """Get the most common host name."""
    host_names = [data.host_name for data in analytics_data]
    return Counter(host_names).most_common(1)[0][0] if host_names else ""

def calculate_language(analytics_data: List[Analytics]) -> str:
    """Get the most common language."""
    languages = [data.language for data in analytics_data]
    return Counter(languages).most_common(1)[0][0] if languages else ""

def calculate_operating_system(analytics_data: List[Analytics]) -> str:
    """Get the most common operating system."""
    operating_systems = [data.operating_system for data in analytics_data]
    return Counter(operating_systems).most_common(1)[0][0] if operating_systems else ""

# Additional functions for calculating city, country, platform, and region
def calculate_browser(analytics_data: List[Analytics]) -> str:
    """Get the most common browser."""
    browsers = [data.browser for data in analytics_data]
    return Counter(browsers).most_common(1)[0][0] if browsers else ""

def calculate_city(analytics_data: List[Analytics]) -> str:
    """Get the most common city."""
    cities = [data.city for data in analytics_data]
    return Counter(cities).most_common(1)[0][0] if cities else ""

def calculate_country(analytics_data: List[Analytics]) -> str:
    """Get the most common country."""
    countries = [data.country for data in analytics_data]
    return Counter(countries).most_common(1)[0][0] if countries else ""

def calculate_platform(analytics_data: List[Analytics]) -> str:
    """Get the most common platform."""
    platforms = [data.platform for data in analytics_data]
    return Counter(platforms).most_common(1)[0][0] if platforms else ""

def calculate_region(analytics_data: List[Analytics]) -> str:
    """Get the most common region."""
    regions = [data.region for data in analytics_data]
    return Counter(regions).most_common(1)[0][0] if regions else ""




# Home route
@router.get("/", response_class=HTMLResponse, name="home")
async def read_root(request: Request):
    logger.info("Rendering home page")
    templates = request.app.state.templates
    return templates.TemplateResponse("home.html", {"request": request})

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
    
@router.get('/api/view_analytics_database')
async def api_analytics():
    try:
        listings_data = get_analytics_from_db()
        return {"analytics":listings_data}
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