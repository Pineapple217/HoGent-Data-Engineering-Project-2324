from .base import Base

import logging
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column, relationship
from sqlalchemy import String, DateTime, Integer, Date, ForeignKey, text
from sqlalchemy.orm import sessionmaker
from repository.main import get_engine, DATA_PATH
import pandas as pd
import numpy as np
from tqdm import tqdm
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from .visits import Visit
    from .campagne import Campagne
    from .contactfiche import Contactfiche

BATCH_SIZE = 10_000
DATE_FORMAT = "%d/%m/%Y"

logger = logging.getLogger(__name__)


class Pageview(Base):
    __tablename__ = "Pageviews"
    __table_args__ = {"extend_existing": True}
    PageViewId: Mapped[str] = mapped_column(String(200), primary_key=True)
    AnonymousVisitor: Mapped[str] = mapped_column(String(50), nullable=True)
    Browser: Mapped[str] = mapped_column(String(50), nullable=True)
    Duration: Mapped[int] = mapped_column(Integer, nullable=True)
    OperatingSystem: Mapped[str] = mapped_column(String(50))
    ReferrerType: Mapped[str] = mapped_column(String(50))
    Time: Mapped[DateTime] = mapped_column(DateTime)
    PageTitle: Mapped[str] = mapped_column(String(1000), nullable=True)
    Type: Mapped[str] = mapped_column(String(200))
    Url: Mapped[str] = mapped_column(String(1000))
    ViewedOn: Mapped[Date] = mapped_column(Date)
    VisitorKey: Mapped[str] = mapped_column(String(200))
    WebContent: Mapped[str] = mapped_column(String(200), nullable=True)
    AangemaaktOp: Mapped[Date] = mapped_column(Date)
    GewijzigdDoor: Mapped[str] = mapped_column(String(200))
    GewijzigdOp: Mapped[Date] = mapped_column(Date)
    Status: Mapped[str] = mapped_column(String(50))
    RedenVanStatus: Mapped[str] = mapped_column(String(50))

    # FK 
    ContactId: Mapped[str] = mapped_column(String(255), ForeignKey('Contactfiche.ContactpersoonId', use_alter=True), nullable=True)
    Contact: Mapped["Contactfiche"] = relationship(back_populates='Pageviews')

    VisitId: Mapped[Optional[str]] = mapped_column(ForeignKey("Visits.VisitId", use_alter=True), nullable=True)
    Visit: Mapped["Visit"] = relationship(back_populates="Pageviews")

    CampagneId: Mapped[Optional[str]] = mapped_column(ForeignKey("Campagne.CampagneId", use_alter=True), nullable=True)
    Campagne: Mapped["Campagne"] = relationship(back_populates="Pageviews")
    

def insert_pageviews_data(pageviews_data, session):
    session.bulk_save_objects(pageviews_data)
    session.commit()


def seed_pageviews():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    
    logger.info("Reading CSV...")
    csv = DATA_PATH + "/cdi_pageviews.csv"
    chunks = pd.read_csv(csv, delimiter=",", encoding="utf-8", keep_default_na=True, na_values=[""], chunksize=50_000, low_memory=False)
    df = pd.concat(chunks)
    
    # Sommige lege waardes worden als NaN ingelezen
    # NaN mag niet in een varchar
    df = df.replace({np.nan: None})
    
    df["crm_CDI_PageView_Viewed_On"] = pd.to_datetime(df["crm_CDI_PageView_Viewed_On"], format=DATE_FORMAT)
    df["crm_CDI_PageView_Time"] = pd.to_datetime(df["crm_CDI_PageView_Time"], format="%m-%d-%Y %H:%M:%S (%Z)")
    df["crm_CDI_PageView_Aangemaakt_op"] = pd.to_datetime(df["crm_CDI_PageView_Aangemaakt_op"], format=DATE_FORMAT)
    df["crm_CDI_PageView_Gewijzigd_op"] = pd.to_datetime(df["crm_CDI_PageView_Gewijzigd_op"], format=DATE_FORMAT)

    pageviews_data = []
    logger.info("Seeding inserting rows")
    progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)
    for _, row in df.iterrows():
        p = Pageview(
            AnonymousVisitor=row["crm_CDI_PageView_Anonymous_Visitor"],
            Browser=row["crm_CDI_PageView_Browser"],
            CampagneId=row["crm_CDI_PageView_Campaign"],
            ContactId=row["crm_CDI_PageView_Contact"],
            Duration=row["crm_CDI_PageView_Duration"],
            OperatingSystem=row["crm_CDI_PageView_Operating_System"],
            PageViewId=row["crm_CDI_PageView_Page_View"],
            ReferrerType=row["crm_CDI_PageView_Referrer_Type"],
            Time=row["crm_CDI_PageView_Time"],
            PageTitle=row["crm_CDI_PageView_Page_Title"],
            Type=row["crm_CDI_PageView_Type"],
            Url=row["crm_CDI_PageView_Url"],
            ViewedOn=row["crm_CDI_PageView_Viewed_On"],
            VisitId=row["crm_CDI_PageView_Visit"],
            VisitorKey=row["crm_CDI_PageView_Visitor_Key"],
            WebContent=row["crm_CDI_PageView_Web_Content"],
            AangemaaktOp=row["crm_CDI_PageView_Aangemaakt_op"],
            GewijzigdDoor=row["crm_CDI_PageView_Gewijzigd_door"],
            GewijzigdOp=row["crm_CDI_PageView_Gewijzigd_op"],
            Status=row["crm_CDI_PageView_Status"],
            RedenVanStatus=row["crm_CDI_PageView_Reden_van_status"],
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
    
    session.execute(text("""
        UPDATE Pageviews
        SET PageViews.VisitId = NULL
        WHERE Pageviews.VisitId
        NOT IN
        (SELECT VisitId FROM Visits)
    """))
    session.commit()

    session.execute(text("""
        UPDATE Pageviews
        SET PageViews.CampagneId = NULL
        WHERE Pageviews.CampagneId
        NOT IN
        (SELECT CampagneId FROM Campagne)
    """))
    session.commit()

    session.execute(text("""
        UPDATE Pageviews
        SET PageViews.ContactId = NULL
        WHERE Pageviews.ContactId
        NOT IN
        (SELECT ContactpersoonId FROM Contactfiche)
    """))
    session.commit()