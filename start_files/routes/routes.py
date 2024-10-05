from start_files.models.mls.analytics import Analytics, analytics_sessionLocal, get_analytics_from_db, get_sub_table_from_db, SubTable
from google.analytics.data_v1beta.types import DateRange, Metric, Dimension, RunReportRequest
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from start_files.models.mls.rentals_db_section import get_rentals_from_db
from start_files.models.mls.homes_db_section import get_homes_from_db
from start_files.routes.rentals_scripts import sorted_rentals_by_price, start_rentals
from start_files.routes.homes_scripts import sorted_homes_by_price, start_homes
from fastapi import APIRouter, Form, Path, Request, HTTPException, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from sqlalchemy.exc import SQLAlchemyError
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

from sqlalchemy.exc import IntegrityError
from fastapi import HTTPException
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
from google.analytics.data_v1beta import BetaAnalyticsDataClient, RunReportRequest
from google.analytics.data_v1beta.types import DateRange, Metric, Dimension
from fastapi.responses import HTMLResponse
from fastapi import APIRouter, Request
import logging

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    try:
        ip_address = request.client.host
        hostname = request.headers.get('Host', '')

        # Authentication and GA data fetching
        credentials = service_account.Credentials.from_service_account_file('services/yonehomes-50e3014c6fd5.json')
        client = BetaAnalyticsDataClient(credentials=credentials)
        property_id = '461503351'

        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=30)

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
                Dimension(name="city"),
                Dimension(name="country"),
                Dimension(name="date"),
                Dimension(name="language"),
                Dimension(name="pageTitle"),
                Dimension(name="landingPage"),
            ]
        )

        response = client.run_report(ga_request)

        # Database processing
        db = analytics_sessionLocal()

        analytics = db.query(Analytics).filter(
            (Analytics.ip_address == ip_address) & (Analytics.hostname == hostname)
        ).first()
        
        if not analytics:
            analytics = Analytics(ip_address=ip_address, hostname=hostname)
            db.add(analytics)
            db.flush()
        else:
            analytics.last_seen = datetime.now()

        for row in response.rows:
            date = datetime.strptime(row.dimension_values[2].value, "%Y%m%d").date()

            sub_table_entry = SubTable(
                analytics_id=analytics.id,
                ip_address=ip_address,
                hostname=hostname,
                date=date,
                sessions=int(float(row.metric_values[6].value)),
                new_users=int(float(row.metric_values[3].value)),
                active_users=int(float(row.metric_values[0].value)),
                average_session_duration=float(row.metric_values[1].value),
                bounce_rate=float(row.metric_values[2].value),
                screen_page_views_per_session=float(row.metric_values[4].value),
                scrolled_users=int(float(row.metric_values[5].value)),
                user_engagement_time=int(float(row.metric_values[7].value)),
                city=row.dimension_values[0].value,
                country=row.dimension_values[1].value,
                language=row.dimension_values[3].value,
                page_title=row.dimension_values[4].value,
                landing_page=row.dimension_values[5].value,
            )
            db.add(sub_table_entry)

        db.commit()

        dashboard_data = get_dashboard_data(db)
        
        templates = request.app.state.templates
        return templates.TemplateResponse("dashboard.html", {"request": request, "dashboard_data": dashboard_data})
    except Exception as e:
        logger.error(f"Unexpected error in dashboard route: {str(e)}")
        raise HTTPException(status_code=500, detail="An unexpected error occurred")



def get_dashboard_data(db, days: int = 30) -> Dict[str, Any]:
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days)
    
    analytics_data = db.query(SubTable).filter(
        SubTable.date >= start_date,
        SubTable.date <= end_date
    ).all()

    return {
        "total_sessions": calculate_total_sessions(analytics_data), # Total Sessions
        "total_users": calculate_total_users(analytics_data), # Total Users
        "new_users": calculate_new_users(analytics_data), # New Users
        "active_users": calculate_active_users(analytics_data), # Active Users
        "user_engagement_duration": calculate_total_user_engagement_duration(analytics_data), # User Engagement Duration
        "avg_session_duration": calculate_avg_session_duration(analytics_data), # Average Session Duration
        "bounce_rate": calculate_bounce_rate(analytics_data), # Bounce Rate
        "screen_page_views_per_session": calculate_avg_screens_per_session(analytics_data), # Screen Page Views Per Session
        "scrolled_users": calculate_total_scrolled_users(analytics_data), # Scrolled Users
        "city": calculate_city(analytics_data), # City
        "country": calculate_country(analytics_data), # Country
        "language": calculate_language(analytics_data), # Language
        "page_title": calculate_page_title(analytics_data), # Page Title
        "landing_page": calculate_landing_page(analytics_data), # Landing Page
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

def calculate_language(analytics_data: List[Analytics]) -> str:
    """Get the most common language."""
    languages = [data.language for data in analytics_data]
    return Counter(languages).most_common(1)[0][0] if languages else ""


def calculate_city(analytics_data: List[Analytics]) -> str:
    """Get the most common city."""
    cities = [data.city for data in analytics_data]
    return Counter(cities).most_common(1)[0][0] if cities else ""

def calculate_country(analytics_data: List[Analytics]) -> str:
    """Get the most common country."""
    countries = [data.country for data in analytics_data]
    return Counter(countries).most_common(1)[0][0] if countries else ""


def calculate_page_title(analytics_data: List[Analytics]) -> str:
    """Calculate the most common page title."""
    page_titles = [data.page_title for data in analytics_data]
    return Counter(page_titles).most_common(1)[0][0] if page_titles else ""

def calculate_landing_page(analytics_data: List[Analytics]) -> str:
    """Calculate the most common landing page."""
    landing_pages = [data.landing_page for data in analytics_data]
    return Counter(landing_pages).most_common(1)[0][0] if landing_pages else ""

def calculate_user_age_brackets(analytics_data: List[Analytics]) -> Counter:
    """Calculate the count of users for each age bracket."""
    age_brackets = [data.user_age_bracket for data in analytics_data if data.user_age_bracket]
    return Counter(age_brackets)

def calculate_user_genders(analytics_data: List[Analytics]) -> Counter:
    """Calculate the count of users for each gender."""
    genders = [data.user_gender for data in analytics_data if data.user_gender]
    return Counter(genders)


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
        main = get_analytics_from_db()
        sub = get_sub_table_from_db() 

        return {
            "analytics": main,
            "subtable": sub
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

    
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

