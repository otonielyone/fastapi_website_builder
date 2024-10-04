from sqlalchemy import BigInteger, Boolean, Date, create_engine, Column, Integer, String, Float
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import inspect
from os import path
import pandas as pd
import logging
import os
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

DATABASE_URL = "sqlite:///brightscrape/brightmls.db"
analytics_engine = create_engine(DATABASE_URL)
analytics_sessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=analytics_engine)
Base = declarative_base()

class Analytics(Base):
    __tablename__ = "analytics"
    id = Column(Integer, primary_key=True, index=True)
    date = Column(Date)
    sessions = Column(Integer)
    new_users = Column(Integer)
    active_users = Column(Integer)
    average_session_duration = Column(Float)
    bounce_rate = Column(Float)
    screen_page_views_per_session = Column(Float)
    scrolled_users = Column(Integer)
    user_engagement_time = Column(BigInteger)
    browser = Column(String)
    city = Column(String)
    country = Column(String)
    host_name = Column(String)
    language = Column(String)
    operating_system = Column(String)
    platform = Column(String)
    region = Column(String)

def init_analytics_db():
    db_path = 'brightscrape/brightmls.db'
    db_dir = os.path.dirname(db_path)
    os.makedirs(db_dir, exist_ok=True)
    
    if not os.path.exists(db_path):
        print("Creating database file...")
        try:
            Base.metadata.create_all(bind=analytics_engine)
            print("Database and tables created successfully.")
        except SQLAlchemyError as e:
            print(f"An error occurred while creating the database: {e}")
    else:
        print("Database file already exists.")

def check_and_create_original_analytics_table():
    db = analytics_sessionLocal()
    inspector = inspect(db.get_bind())
    if not inspector.has_table('analytics'):
        logger.info("Creating original Analytics table...")
        Base.metadata.create_all(bind=db.get_bind(), tables=[Analytics.__table__])
        logger.info("Original Analytics table created.")

def process_analytics_row(row):
    return {
        "ID": row['id'],
        "DATE": row['date'],
        "SESSIONS": row['sessions'],
        "NEW_USERS": row['new_users'],
        "ACTIVE_USERS": row['active_users'],
        "AVERAGE_SESSION_DURATION": row['average_session_duration'],
        "BOUNCE_RATE": row['bounce_rate'],
        "SCREEN_PAGE_VIEWS_PER_SESSION": row['screen_page_views_per_session'],
        "SCROLLED_USERS": row['scrolled_users'],
        "USER_ENGAGEMENT_TIME": row['user_engagement_time'],
        "BROWSER": row['browser'],
        "CITY": row['city'],
        "COUNTRY": row['country'],
        "HOST_NAME": row['host_name'],
        "LANGUAGE": row['language'],
        "OPERATING_SYSTEM": row['operating_system'],
        "PLATFORM": row['platform'],
        "REGION": row['region'],
    }


def get_analytics_from_db():
    db = None
    try:
        db = analytics_sessionLocal()
        listings = db.query(Analytics).all()
        
        listings_dict = [listing.__dict__ for listing in listings]
        df = pd.DataFrame(listings_dict)
   
        with ThreadPoolExecutor() as executor:
            formatted_listings = list(executor.map(process_analytics_row, df.to_dict(orient='records')))
        
        return formatted_listings
    except Exception:
        print(f"An error occurred while retrieving listings")
        return []
    finally:
        if db:
            db.close()

#print(get_analytics_from_db())
