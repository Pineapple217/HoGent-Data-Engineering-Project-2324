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


BATCH_SIZE = 10_000

logger = logging.getLogger(__name__)


class Pageview(Base):
    __tablename__ = "Pageviews"
    __table_args__ = {"extend_existing": True}
    Id: Mapped[int] = mapped_column(
        primary_key=True
    )  # Gebruik deze key voor iedere table
    AnonymousVisitor: Mapped[str] = mapped_column(String(50), nullable=True)
    Browser: Mapped[str] = mapped_column(String(50), nullable=True)
    Campaign: Mapped[str] = mapped_column(String(200), nullable=True)
    Contact: Mapped[str] = mapped_column(String(200), nullable=True)
    Duration: Mapped[int] = mapped_column(Integer, nullable=True)
    OperatingSystem: Mapped[str] = mapped_column(String(50), nullable=True)
    PageView: Mapped[str] = mapped_column(String(200), nullable=True)
    ReferrerType: Mapped[str] = mapped_column(String(50), nullable=True)
    Time: Mapped[DateTime] = mapped_column(DateTime, nullable=True)
    PageTitle: Mapped[str] = mapped_column(String(1000), nullable=True)
    Type: Mapped[str] = mapped_column(String(200), nullable=True)
    Url: Mapped[str] = mapped_column(String(1000), nullable=True)
    ViewedOn: Mapped[Date] = mapped_column(Date, nullable=True)
    Visit: Mapped[str] = mapped_column(String(200), nullable=True)
    VisitorKey: Mapped[str] = mapped_column(String(200), nullable=True)
    WebContent: Mapped[str] = mapped_column(String(200), nullable=True)
    AangemaaktOp: Mapped[Date] = mapped_column(Date, nullable=True)
    GewijzigdDoor: Mapped[str] = mapped_column(String(200), nullable=True)
    GewijzigdOp: Mapped[Date] = mapped_column(Date, nullable=True)
    Status: Mapped[str] = mapped_column(String(50), nullable=True)
    RedenVanStatus: Mapped[str] = mapped_column(String(50), nullable=True)


def insert_pageviews_data(pageviews_data, session):
    session.bulk_save_objects(pageviews_data)
    session.commit()


def seed_pageviews():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    logger.info("Reading CSV...")
    csv = DATA_PATH + "/cdi_pageviews.csv"
    df = pd.read_csv(
        csv, delimiter=";", encoding="latin-1", keep_default_na=True, na_values=[""]
    )
    df = df.replace({np.nan: None})
    # Sommige lege waardes worden als NaN ingelezeno
    # NaN mag niet in een varchar
    df["crm CDI_PageView[Viewed On]"] = pd.to_datetime(
        df["crm CDI_PageView[Viewed On]"], format="%d/%m/%Y"
    )
    df["crm CDI_PageView[Time]"] = pd.to_datetime(
        df["crm CDI_PageView[Time]"], format="%m-%d-%Y %H:%M:%S (%Z)"
    )
    df["crm CDI_PageView[Aangemaakt op]"] = pd.to_datetime(
        df["crm CDI_PageView[Aangemaakt op]"], format="%d/%m/%Y"
    )
    df["crm CDI_PageView[Gewijzigd op]"] = pd.to_datetime(
        df["crm CDI_PageView[Gewijzigd op]"], format="%d/%m/%Y"
    )
    pageviews_data = []
    logger.info("Seeding inserting rows")
    progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)
    for _, row in df.iterrows():
        p = Pageview(
            AnonymousVisitor=row["crm CDI_PageView[Anonymous Visitor]"],
            Browser=row["crm CDI_PageView[Browser]"],
            Campaign=row["crm CDI_PageView[Campaign]"],
            Contact=row["crm CDI_PageView[Contact]"],
            Duration=row["crm CDI_PageView[Duration]"],
            OperatingSystem=row["crm CDI_PageView[Operating System]"],
            PageView=row["crm CDI_PageView[Page View]"],
            ReferrerType=row["crm CDI_PageView[Referrer Type]"],
            Time=row["crm CDI_PageView[Time]"],
            PageTitle=row["crm CDI_PageView[Page Title]"],
            Type=row["crm CDI_PageView[Type]"],
            Url=row["crm CDI_PageView[Url]"],
            ViewedOn=row["crm CDI_PageView[Viewed On]"],
            Visit=row["crm CDI_PageView[Visit]"],
            VisitorKey=row["crm CDI_PageView[Visitor Key]"],
            WebContent=row["crm CDI_PageView[Web Content]"],
            AangemaaktOp=row["crm CDI_PageView[Aangemaakt op]"],
            GewijzigdDoor=row["crm CDI_PageView[Gewijzigd door]"],
            GewijzigdOp=row["crm CDI_PageView[Gewijzigd op]"],
            Status=row["crm CDI_PageView[Status]"],
            RedenVanStatus=row["crm CDI_PageView[Reden van status]"],
        )
        pageviews_data.append(p)

        if len(pageviews_data) >= BATCH_SIZE:
            insert_pageviews_data(pageviews_data, session)
            pageviews_data = []
            progress_bar.update(BATCH_SIZE)

    # Insert any remaining data
    if pageviews_data:
        insert_pageviews_data(pageviews_data, session)
        progress_bar.update(len(pageviews_data))
