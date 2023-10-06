from .base import Base

import logging
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy import String, DateTime, Integer, Date
from sqlalchemy.orm import sessionmaker
from repository.main import get_engine, DATA_PATH
import pandas as pd
import numpy as np
from tqdm import tqdm
import concurrent.futures
import os

NUM_THREADS = 2
BATCH_SIZE = 10_000

logger = logging.getLogger(__name__)

class Pageview(Base):
    __tablename__ = "Pageviews" # snakecase
    __table_args__ = {'extend_existing': True}
    Id: Mapped[int] = mapped_column(primary_key=True) # Gebruik deze key voor iedere table
    Anonymous_Visitor: Mapped[str] = mapped_column(String(50), nullable=True)
    Browser: Mapped[str] = mapped_column(String(50), nullable=True)
    Campaign: Mapped[str] = mapped_column(String(200), nullable=True)
    Contact: Mapped[str] = mapped_column(String(200), nullable=True)
    Duration: Mapped[int] = mapped_column(Integer, nullable=True)
    Operating_System: Mapped[str] = mapped_column(String(50), nullable=True)
    Page_View: Mapped[str] = mapped_column(String(200), nullable=True)
    Referrer_Type: Mapped[str] = mapped_column(String(50), nullable=True)
    Time: Mapped[DateTime] = mapped_column(DateTime, nullable=True)
    Page_Title: Mapped[str] = mapped_column(String(200), nullable=True)
    Type: Mapped[str] = mapped_column(String(200), nullable=True)
    Url: Mapped[str] = mapped_column(String(1000), nullable=True)
    Viewed_On: Mapped[Date] = mapped_column(Date, nullable=True)
    Visit: Mapped[str] = mapped_column(String(200), nullable=True)
    Visitor_Key: Mapped[str] = mapped_column(String(200), nullable=True)
    Web_Content: Mapped[str] = mapped_column(String(200), nullable=True)
    Aangemaakt_op: Mapped[Date] = mapped_column(Date, nullable=True)
    Gewijzigd_door: Mapped[str] = mapped_column(String(200), nullable=True)
    Gewijzigd_op: Mapped[Date] = mapped_column(Date, nullable=True)
    Status: Mapped[str] = mapped_column(String(50), nullable=True)
    Reden_van_status: Mapped[str] = mapped_column(String(50), nullable=True)

def insert_pageviews_data(pageviews_data, session):
    session.bulk_save_objects(pageviews_data)
    session.commit()

def seed_pageviews():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    logger.info("Reading CSV...")
    csv = DATA_PATH + '/cdi_pageviews.csv'
    df = pd.read_csv(csv, delimiter=";",encoding='utf-8', keep_default_na=True, na_values=[''])
    df = df.replace({np.nan: None})
    # Sommige lege waardes worden als NaN ingelezeno
    # NaN mag niet in een varchar
    df['crm CDI_PageView[Viewed On]'] = pd.to_datetime(df['crm CDI_PageView[Viewed On]'], format='%d/%m/%Y')
    df['crm CDI_PageView[Time]'] = pd.to_datetime(df['crm CDI_PageView[Time]'], format='%m-%d-%Y %H:%M:%S (%Z)')
    df['crm CDI_PageView[Aangemaakt op]'] = pd.to_datetime(df['crm CDI_PageView[Aangemaakt op]'], format='%d/%m/%Y')
    df['crm CDI_PageView[Gewijzigd op]'] = pd.to_datetime(df['crm CDI_PageView[Gewijzigd op]'], format='%d/%m/%Y')
    pageviews_data = []
    logger.info("Seeding inserting rows")
    progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = []
        for _, row in df.iterrows():
            p = Pageview(
                Anonymous_Visitor=row['crm CDI_PageView[Anonymous Visitor]'],
                Browser=row['crm CDI_PageView[Browser]'],
                Campaign=row['crm CDI_PageView[Campaign]'],
                Contact=row['crm CDI_PageView[Contact]'],
                Duration=row['crm CDI_PageView[Duration]'],
                Operating_System=row['crm CDI_PageView[Operating System]'],
                Page_View=row['crm CDI_PageView[Page View]'],
                Referrer_Type=row['crm CDI_PageView[Referrer Type]'],
                Time=row['crm CDI_PageView[Time]'],
                Page_Title=row['crm CDI_PageView[Page Title]'],
                Type=row['crm CDI_PageView[Type]'],
                Url=row['crm CDI_PageView[Url]'],
                Viewed_On=row['crm CDI_PageView[Viewed On]'],
                Visit=row['crm CDI_PageView[Visit]'],
                Visitor_Key=row['crm CDI_PageView[Visitor Key]'],
                Web_Content=row['crm CDI_PageView[Web Content]'],
                Aangemaakt_op=row['crm CDI_PageView[Aangemaakt op]'],
                Gewijzigd_door=row['crm CDI_PageView[Gewijzigd door]'],
                Gewijzigd_op=row['crm CDI_PageView[Gewijzigd op]'],
                Status=row['crm CDI_PageView[Status]'],
                Reden_van_status=row['crm CDI_PageView[Reden van status]']
            )
            pageviews_data.append(p)
            
            if len(pageviews_data) >= BATCH_SIZE:
                futures.append(executor.submit(insert_pageviews_data, pageviews_data, session))
                pageviews_data = []
                progress_bar.update(BATCH_SIZE)

        # Insert any remaining data
        if pageviews_data:
            futures.append(executor.submit(insert_pageviews_data, pageviews_data, session))

        concurrent.futures.wait(futures)
