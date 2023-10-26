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

logger = logging.getLogger(__name__)


class Pageview(Base):
    __tablename__ = "Pageviews"
    __table_args__ = {"extend_existing": True}
    PageView: Mapped[str] = mapped_column(String(200), primary_key=True)
    AnonymousVisitor: Mapped[str] = mapped_column(String(50), nullable=True)
    Browser: Mapped[str] = mapped_column(String(50), nullable=True)
    Contact: Mapped[str] = mapped_column(String(255), ForeignKey('Contactfiche.ContactPersoon', use_alter=True), nullable=True)
    contactFK: Mapped["Contactfiche"] = relationship("Contactfiche", backref="FKPageviewsContact")
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

    VisitId: Mapped[Optional[str]] = mapped_column(ForeignKey("Visits.Visit", use_alter=True), nullable=True)
    Visit: Mapped["Visit"] = relationship(back_populates="Pageviews")

    CampagneId: Mapped[Optional[str]] = mapped_column(ForeignKey("Campagne.Id", use_alter=True), nullable=True)
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
    chunks = pd.read_csv(
        csv, delimiter=";", encoding="latin-1", keep_default_na=True, na_values=[""], chunksize=50_000
    )
    df = pd.concat(chunks) 
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
            CampagneId=row["crm CDI_PageView[Campaign]"],
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
            VisitId=row["crm CDI_PageView[Visit]"],
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
    
    session.execute(text("""
        UPDATE Pageviews
        SET PageViews.VisitId = NULL
        WHERE Pageviews.VisitId
        NOT IN
        (SELECT Visit FROM Visits)
    """))
    session.commit()

    session.execute(text("""
        UPDATE Pageviews
        SET PageViews.CampagneId = NULL
        WHERE Pageviews.CampagneId
        NOT IN
        (SELECT Id FROM Campagne)
    """))
    session.commit()

    session.execute(text("""
        UPDATE Pageviews
        SET PageViews.Contact = NULL
        WHERE Pageviews.Contact
        NOT IN
        (SELECT ContactPersoon FROM Contactfiche)
    """))
    session.commit()