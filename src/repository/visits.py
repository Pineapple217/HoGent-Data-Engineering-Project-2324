from .base import Base

import logging
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column, relationship
from sqlalchemy import String, DateTime, Integer, Boolean, Float, ForeignKey, text
from sqlalchemy.orm import sessionmaker
from repository.main import get_engine, DATA_PATH
import pandas as pd
import numpy as np
from typing import Optional, TYPE_CHECKING
from tqdm import tqdm

if TYPE_CHECKING:
    from .mailing import Mailing
    from .pageviews import Pageview
    from .contactfiche import Contactfiche

BATCH_SIZE = 10_000
DATE_FORMAT = "%d-%m-%Y %H:%M:%S"

logger = logging.getLogger(__name__)


class Visit(Base):
    __tablename__ = "Visits"
    __table_args__ = {"extend_existing": True}
    VisitId                 :Mapped[str] = mapped_column(String(50), primary_key=True)
    AdobeReader             :Mapped[bool] = mapped_column(Boolean)
    Bounce                  :Mapped[bool] = mapped_column(Boolean)
    Browser                 :Mapped[str] = mapped_column(String(50), nullable=True)
    CampagneCode            :Mapped[int] = mapped_column(Integer, nullable=True)
    Campaign                :Mapped[str] = mapped_column(String(50), nullable=True)
    IPStad                  :Mapped[str] = mapped_column(String(50), nullable=True)
    IPCompany               :Mapped[str] = mapped_column(String(200), nullable=True)
    ContactNaam             :Mapped[str] = mapped_column(String(200))
    ContainsSocialProfile   :Mapped[bool] = mapped_column(Boolean)
    IPLand                  :Mapped[str] = mapped_column(String(50), nullable=True)
    Duration                :Mapped[float] = mapped_column(Float, nullable=True)
    EndedOn                 :Mapped[DateTime] = mapped_column(DateTime, nullable=True)
    EntryPage               :Mapped[str] = mapped_column(String(1000), nullable=True)
    ExitPage                :Mapped[str] = mapped_column(String(1000), nullable=True)
    FirstVisit              :Mapped[bool] = mapped_column(Boolean)
    IPAddress               :Mapped[str] = mapped_column(String(50), nullable=True)
    IPOrganization          :Mapped[str] = mapped_column(String(50), nullable=True)
    Keywords                :Mapped[str] = mapped_column(String(200), nullable=True)
    IPLatitude              :Mapped[float] = mapped_column(Float, nullable=True)
    IPLongitude             :Mapped[float] = mapped_column(Float, nullable=True)
    OperatingSystem         :Mapped[str] = mapped_column(String(50), nullable=True)
    IPPostcode              :Mapped[str] = mapped_column(String(50), nullable=True)
    Referrer                :Mapped[str] = mapped_column(String(1000), nullable=True)
    ReferringHost           :Mapped[str] = mapped_column(String(50), nullable=True)
    Score                   :Mapped[float] = mapped_column(Float, nullable=True)
    ReferrerType            :Mapped[str] = mapped_column(String(50))
    StartedOn               :Mapped[DateTime] = mapped_column(DateTime, nullable=True)
    IPStatus                :Mapped[str] = mapped_column(String(50), nullable=True)
    Time                    :Mapped[DateTime] = mapped_column(DateTime, nullable=True)
    TotalPages              :Mapped[int] = mapped_column(Integer, nullable=True)
    AangemaaktOp            :Mapped[DateTime] = mapped_column(DateTime)
    GewijzigdOp             :Mapped[DateTime] = mapped_column(DateTime)

    # FK    
    EmailSendId             :Mapped[Optional[str]] = mapped_column(ForeignKey("Mailing.MailingId"), nullable=True)
    EmailSend               :Mapped[Optional["Mailing"]] = relationship(back_populates="Visits")

    ContactId               :Mapped[str] = mapped_column(String(255), ForeignKey('Contactfiche.ContactpersoonId', use_alter=True), nullable=True)
    Contact                 :Mapped["Contactfiche"] = relationship(back_populates="Visit") 

    Pageviews               :Mapped["Pageview"] = relationship(back_populates="Visit")


def insert_visits_data(pageviews_data, session):
    session.bulk_save_objects(pageviews_data)
    session.commit()


def seed_visits():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    
    logger.info("Reading CSV...")
    csv = DATA_PATH + "/CDI visits.csv"
    chunks = pd.read_csv(csv, delimiter=",", encoding="utf-8", keep_default_na=True, na_values=[""], chunksize=10_000)
    df = pd.concat(chunks)
    
    df['crm_CDI_Visit_Adobe_Reader'] = df['crm_CDI_Visit_Adobe_Reader'].replace({'Ja': True, 'Nee': False})
    df['crm_CDI_Visit_Bounce'] = df['crm_CDI_Visit_Bounce'].replace({'Ja': True, 'Nee': False})
    df['crm_CDI_Visit_containssocialprofile'] = df['crm_CDI_Visit_containssocialprofile'].replace({'Ja': True, 'Nee': False})
    df['crm_CDI_Visit_First_Visit'] = df['crm_CDI_Visit_First_Visit'].replace({'Ja': True, 'Nee': False})

    df['crm_CDI_Visit_Ended_On'] = pd.to_datetime(df['crm_CDI_Visit_Ended_On'], format=DATE_FORMAT)
    df['crm_CDI_Visit_Started_On'] = pd.to_datetime(df['crm_CDI_Visit_Started_On'], format=DATE_FORMAT)
    df['crm_CDI_Visit_Aangemaakt_op'] = pd.to_datetime(df['crm_CDI_Visit_Aangemaakt_op'], format=DATE_FORMAT)
    df['crm_CDI_Visit_Gewijzigd_op'] = pd.to_datetime(df['crm_CDI_Visit_Gewijzigd_op'], format=DATE_FORMAT)
    
    df['crm_CDI_Visit_Time'] = pd.to_datetime(df['crm_CDI_Visit_Time'], format="%m-%d-%Y %H:%M:%S (%Z)")
    
    df = df.replace({np.nan: None})

    visits_data = []
    logger.info("Seeding inserting rows")
    progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)
    for _, row in df.iterrows():
        p = Visit(
            VisitId                 =  row['crm_CDI_Visit_Visit'],
            AdobeReader             =  row['crm_CDI_Visit_Adobe_Reader'],
            Bounce                  =  row['crm_CDI_Visit_Bounce'],
            Browser                 =  row['crm_CDI_Visit_Browser'],
            CampagneCode            =  row['crm_CDI_Visit_Campagne_Code'],
            Campaign                =  row['crm_CDI_Visit_Campaign'],
            IPStad                  =  row['crm_CDI_Visit_IP_Stad'],
            IPCompany               =  row['crm_CDI_Visit_IP_Company'],
            ContactId               =  row['crm_CDI_Visit_Contact'],
            ContactNaam             =  row['crm_CDI_Visit_Contact_Naam_'],
            ContainsSocialProfile   =  row['crm_CDI_Visit_containssocialprofile'],
            IPLand                  =  row['crm_CDI_Visit_IP_Land'],
            Duration                =  row['crm_CDI_Visit_Duration'],
            EmailSendId             =  row['crm_CDI_Visit_Email_Send'],
            EndedOn                 =  row['crm_CDI_Visit_Ended_On'],
            EntryPage               =  row['crm_CDI_Visit_Entry_Page'],
            ExitPage                =  row['crm_CDI_Visit_Exit_Page'],
            FirstVisit              =  row['crm_CDI_Visit_First_Visit'],
            IPAddress               =  row['crm_CDI_Visit_IP_Address'],
            IPOrganization          =  row['crm_CDI_Visit_IP_Organization'],
            Keywords                =  row['crm_CDI_Visit_Keywords'],
            IPLatitude              =  row['crm_CDI_Visit_IP_Latitude'],
            IPLongitude             =  row['crm_CDI_Visit_IP_Longitude'],
            OperatingSystem         =  row['crm_CDI_Visit_Operating_System'],
            IPPostcode              =  row['crm_CDI_Visit_IP_Postcode'],
            Referrer                =  row['crm_CDI_Visit_Referrer'],
            ReferringHost           =  row['crm_CDI_Visit_Referring_Host'],
            Score                   =  row['crm_CDI_Visit_Score'],
            ReferrerType            =  row['crm_CDI_Visit_Referrer_Type'],
            StartedOn               =  row['crm_CDI_Visit_Started_On'],
            IPStatus                =  row['crm_CDI_Visit_IP_Status'],
            Time                    =  row['crm_CDI_Visit_Time'],
            TotalPages              =  row['crm_CDI_Visit_Total_Pages'],
            AangemaaktOp            =  row['crm_CDI_Visit_Aangemaakt_op'],
            GewijzigdOp             =  row['crm_CDI_Visit_Gewijzigd_op'],
        )
        visits_data.append(p)

        if len(visits_data) >= BATCH_SIZE:
            insert_visits_data(visits_data, session)
            visits_data = []
            progress_bar.update(BATCH_SIZE)

    # Insert any remaining data
    if visits_data:
        insert_visits_data(visits_data, session)
        progress_bar.update(len(visits_data))

    session.execute(text("""
        UPDATE Visits
        SET Visits.EmailSendId = NULL
        WHERE Visits.EmailSendId
        NOT IN
        (SELECT MailingId FROM Mailing);
    """))
    session.commit()

    session.execute(text("""
        UPDATE Visits
        SET Visits.ContactId = NULL
        WHERE Visits.ContactId
        NOT IN
        (SELECT ContactpersoonId FROM Contactfiche)
    """))
    session.commit()