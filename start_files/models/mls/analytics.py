from sqlalchemy import BigInteger, Boolean, Date, DateTime, create_engine, Column, Integer, String, Float, UniqueConstraint, func
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import inspect
from os import path
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
import pandas as pd
from sqlalchemy import ForeignKey
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
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    ip_address = Column(String, nullable=False, index=True)
    hostname = Column(String, nullable=False, index=True)
    first_seen = Column(DateTime, default=func.now())
    last_seen = Column(DateTime, default=func.now(), onupdate=func.now())
    
    __table_args__ = (UniqueConstraint('ip_address', 'hostname', name='uq_ip_hostname'),)
    
    sub_info = relationship("SubTable", back_populates="main_info", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<Analytics(ip_address={self.ip_address}, hostname={self.hostname}, first_seen={self.first_seen}, last_seen={self.last_seen})>"

class SubTable(Base):
    __tablename__ = 'sub_table'
    
    id = Column(Integer, primary_key=True, index=True)
    analytics_id = Column(Integer, ForeignKey('analytics.id'))
    ip_address = Column(String, index=True)
    hostname = Column(String, index=True)
    date = Column(Date)
    sessions = Column(Integer)
    new_users = Column(Integer)
    active_users = Column(Integer)
    average_session_duration = Column(Float)
    bounce_rate = Column(Float)
    screen_page_views_per_session = Column(Float)
    scrolled_users = Column(Integer)
    user_engagement_time = Column(BigInteger)
    city = Column(String)
    country = Column(String)
    language = Column(String)
    page_title = Column(String)
    landing_page = Column(String)
    user_age_bracket = Column(String)
    user_gender = Column(String)

    main_info = relationship("Analytics", back_populates="sub_info")

    def __repr__(self):
        return f"<SubTable(analytics_id={self.analytics_id}, date={self.date}, sessions={self.sessions})>"

def init_db():
    Base.metadata.create_all(bind=analytics_engine)

def get_db():
    db = analytics_sessionLocal()
    try:
        yield db
    finally:
        db.close()

# Initialize the database
init_db()



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

# Check and create analytics table
def check_and_create_analytics_table():
    db = analytics_sessionLocal()
    inspector = inspect(db.get_bind())
    if not inspector.has_table('analytics'):
        logger.info("Creating Analytics table...")
        Base.metadata.create_all(bind=db.get_bind(), tables=[Analytics.__table__])
        logger.info("Analytics table created.")
    if not inspector.has_table('sub_table'):
        logger.info("Creating sub_table table...")
        Base.metadata.create_all(bind=db.get_bind(), tables=[SubTable.__table__])
        logger.info("sub_table table created.")


# Check and drop analytics and sub_table
def drop_tables():
    db = analytics_sessionLocal()
    inspector = inspect(db.get_bind())
    
    if inspector.has_table('analytics'):
        logger.info("Dropping Analytics table...")
        Analytics.__table__.drop(db.get_bind())
        logger.info("Analytics table dropped.")
        
    if inspector.has_table('sub_table'):
        logger.info("Dropping sub_table...")
        SubTable.__table__.drop(db.get_bind())
        logger.info("sub_table dropped.")


# Check and create analytics table
def check_and_create_analytics_table():
    db = analytics_sessionLocal()
    inspector = inspect(db.get_bind())
    if not inspector.has_table('analytics'):
        logger.info("Creating Analytics table...")
        Base.metadata.create_all(bind=db.get_bind(), tables=[Analytics.__table__])
        logger.info("Analytics table created.")
    if not inspector.has_table('sub_table'):
        logger.info("Creating sub_table table...")
        Base.metadata.create_all(bind=db.get_bind(), tables=[SubTable.__table__])
        logger.info("sub_table table created.")

import pandas as pd
from concurrent.futures import ThreadPoolExecutor

# Process analytics row
def process_subtable_row(row):
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
        "CITY": row['city'],
        "COUNTRY": row['country'],
        "LANGUAGE": row['language'],
        "PAGE_TITLE": row['page_title'],
        "LANDING_PAGE": row['landing_page'],
        "USER_AGE_BRACKET": row['user_age_bracket'],
        "USER_GENDER": row['user_gender'],
    }

# Process analytics row
def process_analytics_row(row):
    return {
        "ID": row['id'],
        "IP_ADDRESS": row['ip_address'],
        "HOSTNAME": row['hostname'],
    }

# Get analytics from DB
def get_analytics_from_db():
    db = None
    try:
        db = analytics_sessionLocal()
        listings = db.query(Analytics).all()

        listings_dict = [listing.__dict__ for listing in listings]
        df = pd.DataFrame(listings_dict)

        with ThreadPoolExecutor() as executor:
            analytics_data = list(executor.map(process_analytics_row, df.to_dict(orient='records')))
                
        return analytics_data
    except Exception as e:
        print(f"An error occurred while retrieving listings: {e}")
        return [], []
    finally:
        if db:
            db.close()


def get_sub_table_from_db():
    db = None
    try:
        db = analytics_sessionLocal()
        listings = db.query(SubTable).all()

        listings_dict = [listing.__dict__ for listing in listings]
        df = pd.DataFrame(listings_dict)

        with ThreadPoolExecutor() as executor:
            sub_table_data = list(executor.map(process_subtable_row, df.to_dict(orient='records')))
                
        return sub_table_data
    except Exception as e:
        print(f"An error occurred while retrieving listings: {e}")
        return [], []
    finally:
        if db:
            db.close()



#drop_tables()
#check_and_create_analytics_table()
