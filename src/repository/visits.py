from .base import Base
from .functionalities import load_csv, move_csv_file

import logging
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column, relationship
from sqlalchemy import String, DateTime, Integer, Boolean, Float, ForeignKey, text
from sqlalchemy.orm import sessionmaker
from repository.main import get_engine, DATA_PATH
import os
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


'''
USAGE:
Create the directories "old" and "new" in the data folder.
Place new CSV file in the "new" folder.
Run the seeding.
The processed CSV file will be moved to the "old" folder with a timestamp.
The "new" folder will now be empty.
Place a new CSV file in the "new" folder to add more new data.
Run the seeding again.
'''


# geeft een lijst van alle ids die al in de database zitten door de tabel te queryen, enkel de id kolom wordt teruggegeven
def get_existing_ids(session):
    return [result[0] for result in session.query(Visit.VisitId).all()]


def seed_visits():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    
    # haal alle ids op die al in de database zitten
    existing_ids = get_existing_ids(session)
    
    # pad naar de csv bestanden, deze moeten in de mappen "old" en "new" zitten
    old_csv_dir = os.path.join(DATA_PATH, "old")
    new_csv_dir = os.path.join(DATA_PATH, "new")
    
    # check of de mappen "old" en "new" bestaan
    if not os.path.exists(old_csv_dir) or not os.path.exists(new_csv_dir):
        raise FileNotFoundError("The folders 'old' and 'new' must exist in the data folder")

    folder_new = new_csv_dir
    visits_data = []
    
    # enkel de csv bestanden in de "new" map worden ingelezen, de bestanden in de "old" map zijn reeds verwerkt
    for filename in os.listdir(folder_new):
        if filename == 'CDI visits.csv':
            csv_path = os.path.join(folder_new, filename)
            logger.info(f"Reading CSV: {csv_path}")
            df, error = load_csv(csv_path)
            if error:
                raise Exception(f"Error loading CSV: {csv_path, error}")
            
            # verwijder de rijen waarvan de id al in de database zit
            df = df[~df["crm_CDI_Visit_Visit"].isin(existing_ids)]
            
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
            
            logger.info("Seeding inserting rows")
            progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)
            
            # data in chunks steken
            chunks = [df[i:i + BATCH_SIZE] for i in range(0, df.shape[0], BATCH_SIZE)]
            for chunk in chunks:
                visits_data = []
                for _, row in chunk.iterrows(): 
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

                insert_visits_data(visits_data, session)
                progress_bar.update(len(visits_data))

            progress_bar.close()

            # verplaats het csv bestand naar de "old" map voor reeds verwerkte bestanden
            move_csv_file(csv_path, old_csv_dir)

            logger.info(f"Number of new (non-duplicate) rows found in {csv_path}: {len(df)}")
    
    # als er geen nieuwe data is, dan is de lijst leeg
    if not visits_data:
        logger.info("No new data was given. Data is up to date already.")
        
    session.execute(text("""
        UPDATE Visits
        SET Visits.EmailSendId = NULL
        WHERE Visits.EmailSendId IS NOT NULL
        AND Visits.EmailSendId NOT IN
        (SELECT MailingId FROM Mailing WHERE MailingId IS NOT NULL);
    """))
    session.commit()

    session.execute(text("""
        UPDATE Visits
        SET Visits.ContactId = NULL
        WHERE Visits.ContactId IS NOT NULL
        AND Visits.ContactId NOT IN
        (SELECT ContactpersoonId FROM Contactfiche WHERE ContactpersoonId IS NOT NULL);
    """))
    session.commit()