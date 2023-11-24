from .base import Base
from .functionalities import load_csv, move_csv_file

import logging
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column, relationship
from sqlalchemy import String, DateTime, Integer, Date, ForeignKey, text
from sqlalchemy.orm import sessionmaker
from repository.main import get_engine, DATA_PATH
import os
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


# geeft een lijst van alle campagne ids die al in de database zitten door de campagne tabel te queryen, enkel de campagne id kolom wordt teruggegeven
def get_existing_ids(session):
    return [result[0] for result in session.query(Pageview.PageViewId).all()]

def seed_pageviews():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    
    # haal alle campagne ids op die al in de database zitten
    existing_ids = get_existing_ids(session)
    
    # pad naar de csv bestanden, deze moeten in de mappen "old" en "new" zitten
    old_csv_dir = os.path.join(DATA_PATH, "old")
    new_csv_dir = os.path.join(DATA_PATH, "new")
    
    # check of de mappen "old" en "new" bestaan
    if not os.path.exists(old_csv_dir) or not os.path.exists(new_csv_dir):
        raise FileNotFoundError("The folders 'old' and 'new' must exist in the data folder")

    folder_new = new_csv_dir
    pageviews_data = []
    
    # enkel de csv bestanden in de "new" map worden ingelezen, de bestanden in de "old" map zijn reeds verwerkt
    for filename in os.listdir(folder_new):
        if filename == 'cdi_pageviews.csv':
            csv_path = os.path.join(folder_new, filename)
            logger.info(f"Reading CSV: {csv_path}")
            df, error = load_csv(csv_path)
            if error:
                raise Exception(f"Error loading CSV: {csv_path, error}")
            
            # geeft de dataframe consistente kolomnamen, volgens het originele csv bestand
            df.columns = [
                "crm_CDI_PageView_Anonymous_Visitor",
                "crm_CDI_PageView_Browser",
                "crm_CDI_PageView_Campaign",
                "crm_CDI_PageView_Contact",
                "crm_CDI_PageView_Duration",
                "crm_CDI_PageView_Operating_System",
                "crm_CDI_PageView_Page_View",
                "crm_CDI_PageView_Referrer_Type",
                "crm_CDI_PageView_Time",
                "crm_CDI_PageView_Page_Title",
                "crm_CDI_PageView_Type",
                "crm_CDI_PageView_Url",
                "crm_CDI_PageView_Viewed_On",
                "crm_CDI_PageView_Visit",
                "crm_CDI_PageView_Visitor_Key",
                "crm_CDI_PageView_Web_Content",
                "crm_CDI_PageView_Aangemaakt_op",
                "crm_CDI_PageView_Gewijzigd_door",
                "crm_CDI_PageView_Gewijzigd_op",
                "crm_CDI_PageView_Status",
                "crm_CDI_PageView_Reden_van_status"
            ]

            # verwijder de rijen waarvan de campagne id al in de database zit
            df = df[~df["crm_CDI_PageView_Page_View"].isin(existing_ids)]
            
            # Sommige lege waardes worden als NaN ingelezen
            df = df.replace({np.nan: None})
        
            possible_date_formats = ["%d-%m-%Y %H:%M:%S", "%d/%m/%Y", "%m-%d-%Y %H:%M:%S (%Z)"]

            # kolommen met date(time) waardes
            date_columns = ["crm_CDI_PageView_Viewed_On", "crm_CDI_PageView_Time", "crm_CDI_PageView_Aangemaakt_op", "crm_CDI_PageView_Gewijzigd_op"]

            # de date(time) formats zijn niet consistent tussen de cdi pageviews csv bestanden heen
            for date_column in date_columns:
                # probeer de verschillende date formats tot de juiste is gevonden
                for date_format in possible_date_formats:
                    try:
                        df[date_column] = pd.to_datetime(df[date_column], format=date_format)
                        break  
                    except ValueError:
                        # probeer de volgende date format
                        pass
            
            logger.info("Seeding inserting rows")
            progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)
            
            # data in chunks steken
            chunks = [df[i:i + BATCH_SIZE] for i in range(0, df.shape[0], BATCH_SIZE)]
            for chunk in chunks:
                pageviews_data = []
                for _, row in chunk.iterrows(): 
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
                        RedenVanStatus=row["crm_CDI_PageView_Reden_van_status"]
                    )
                    pageviews_data.append(p)
                    
                insert_pageviews_data(pageviews_data, session)
                progress_bar.update(len(pageviews_data))
                
            progress_bar.close()
            
            # verplaats het csv bestand naar de "old" map voor reeds verwerkte bestanden (timestamp wordt meegegeven)
            move_csv_file(csv_path, old_csv_dir)

            logger.info(f"Number of new (non-duplicate) rows found in {csv_path}: {len(df)}")
            
    # als er geen nieuwe data is, dan is de lijst leeg
    if not pageviews_data:
        logger.info("No new data was given. Data is up to date already.")
    
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
    
    session.close()