from .base import Base

import logging
from sqlalchemy.orm import Mapped, mapped_column, sessionmaker, relationship
from sqlalchemy import String
from sqlalchemy.dialects.mssql import DATETIME2
from repository.main import get_engine, DATA_PATH
import os
import datetime
import pandas as pd
import numpy as np
from typing import TYPE_CHECKING
from tqdm import tqdm

if TYPE_CHECKING:
    from .pageviews import Pageview
    from .sessie import Sessie
    from .inschrijving import Inschrijving

BATCH_SIZE = 10_000
DATE_FORMAT = "%d-%m-%Y %H:%M:%S"

logger = logging.getLogger(__name__)

class Campagne(Base):
    __tablename__ = "Campagne"
    __table_args__ = {"extend_existing": True}
    CampagneId: Mapped[str] = mapped_column(String(50), primary_key=True)
    CampagneNr: Mapped[str] = mapped_column(String(50), nullable=True)
    Einddatum: Mapped[DATETIME2] = mapped_column(DATETIME2)
    CampagneNaam: Mapped[str] = mapped_column(String(200))
    NaamInMail: Mapped[str] = mapped_column(String(200))
    RedenVanStatus: Mapped[str] = mapped_column(String(50))
    Startdatum: Mapped[DATETIME2] = mapped_column(DATETIME2)
    Status: Mapped[str] = mapped_column(String(50))
    TypeCampagne: Mapped[str] = mapped_column(String(50))
    URLVoka: Mapped[str] = mapped_column(String(150), nullable=True)
    SoortCampagne: Mapped[str] = mapped_column(String(50))

    # FK
    Pageviews: Mapped["Pageview"] = relationship(back_populates="Campagne")
    Sessie: Mapped["Sessie"] = relationship(back_populates="Campagne")
    Inschrijving: Mapped["Inschrijving"] = relationship(back_populates="Campagne")

def insert_campagne_data(campagne_data, session):
    session.bulk_save_objects(campagne_data)
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

def move_csv_file(csv_path, destination_folder):
    base_name = os.path.basename(csv_path)
    file_name, file_extension = os.path.splitext(base_name)

    timestamp_str = datetime.datetime.now().strftime("%Y_%m_%d_%H-%M-%S")
    new_path = os.path.join(destination_folder, f"{file_name}_{timestamp_str}{file_extension}")
    
    os.rename(csv_path, new_path)

def get_existing_ids(session):
    return set(session.query(Campagne.CampagneId).all())

def seed_campagne():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    existing_ids = get_existing_ids(session)
    
    old_csv_dir = os.path.join(DATA_PATH, "old")
    new_csv_dir = os.path.join(DATA_PATH, "new")
    
    if not os.path.exists(old_csv_dir) or not os.path.exists(new_csv_dir):
        raise FileNotFoundError("The folders 'old' and 'new' must exist in the data folder")

    folder_new = new_csv_dir

    campagne_data = []
    for filename in os.listdir(folder_new):
        if filename.startswith("Campagne"):
            csv_path = os.path.join(folder_new, filename)

            logger.info(f"Reading CSV: {csv_path}")
            df = pd.read_csv(csv_path, delimiter=",", encoding="utf-8", keep_default_na=True, na_values=[""])

            df = df.replace({np.nan: None})

            df["crm_Campagne_Einddatum"] = pd.to_datetime(df["crm_Campagne_Einddatum"], format=DATE_FORMAT)
            df["crm_Campagne_Startdatum"] = pd.to_datetime(df["crm_Campagne_Startdatum"], format=DATE_FORMAT)
            
            logger.info("Seeding inserting rows")

            progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)

            for _, row in df.iterrows():
                campagne_id = row["crm_Campagne_Campagne"]

                existing_record = session.query(Campagne).filter_by(CampagneId=campagne_id).first()

                if existing_record:
                    continue

                existing_ids.add(campagne_id)

                p = Campagne(
                    CampagneId=row["crm_Campagne_Campagne"],
                    CampagneNr=row["crm_Campagne_Campagne_Nr"],
                    Einddatum=row["crm_Campagne_Einddatum"],
                    CampagneNaam=row["crm_Campagne_Naam"],
                    NaamInMail=row["crm_Campagne_Naam_in_email"],
                    RedenVanStatus=row["crm_Campagne_Reden_van_status"],
                    Startdatum=row["crm_Campagne_Startdatum"],
                    Status=row["crm_Campagne_Status"],
                    TypeCampagne=row["crm_Campagne_Type_campagne"],
                    URLVoka=row["crm_Campagne_URL_voka_be"],
                    SoortCampagne=row["crm_Campagne_Soort_Campagne"],
                )
                campagne_data.append(p)

                if len(campagne_data) >= BATCH_SIZE:
                    insert_campagne_data(campagne_data, session)
                    campagne_data = []
                    progress_bar.update(BATCH_SIZE)

            if campagne_data:
                insert_campagne_data(campagne_data, session)
                progress_bar.update(len(campagne_data))

            progress_bar.close()

            move_csv_file(csv_path, old_csv_dir)

            logger.info(f"Number of new (non-duplicate) rows found in {csv_path}: {len(campagne_data)}")
    
    if not campagne_data:
        logger.info("No new data was given. Data is up to date already.")
        
    session.close()