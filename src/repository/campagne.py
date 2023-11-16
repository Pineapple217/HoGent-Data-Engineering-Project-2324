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

def move_csv_file(csv_path, destination_folder, timestamp=True):
    # verplaats de verwerkte csv naar de old folder met een timestamp om naamconflicten te vermijden  
    if timestamp:
        timestamp_str = datetime.datetime.now().strftime("%Y_W%U")
        base_name = os.path.basename(csv_path)
        new_path = os.path.join(destination_folder, f"{base_name}_{timestamp_str}")
    else:
        new_path = os.path.join(destination_folder, os.path.basename(csv_path))
    
    os.rename(csv_path, new_path)

def seed_campagne():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    # bijhouden welke campagnes al in de database zitten op basis van hun ID (primary key)
    existing_ids = set()

    # maak deze folders aan! verdeel de csv's
    old_csv_dir = os.path.join(DATA_PATH, "old")
    new_csv_dir = os.path.join(DATA_PATH, "new")

    for folder in [old_csv_dir, new_csv_dir]: # loop over beide folders
        logger.info(f"Processing CSV files in '{folder}' folder...")

        for filename in os.listdir(folder):
            if filename.startswith("Campagne"):
                csv_path = os.path.join(folder, filename)

                logger.info(f"Reading CSV: {csv_path}")
                df = pd.read_csv(csv_path, delimiter=",", encoding="utf-8", keep_default_na=True, na_values=[""])

                df = df.replace({np.nan: None})

                df["crm_Campagne_Einddatum"] = pd.to_datetime(df["crm_Campagne_Einddatum"], format=DATE_FORMAT)
                df["crm_Campagne_Startdatum"] = pd.to_datetime(df["crm_Campagne_Startdatum"], format=DATE_FORMAT)

                # laat de rijen vallen die al in de database zitten
                df_no_duplicates = df[~df["crm_Campagne_Campagne"].isin(existing_ids)]
                # werk de set van de bestaande campagne ID's bij
                existing_ids.update(df_no_duplicates["crm_Campagne_Campagne"])

                # hou bij hoeveel nieuwe rijen er zijn
                new_rows_count = len(df_no_duplicates)

                campagne_data = []
                logger.info("Seeding inserting rows")
                progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)
                for _, row in df_no_duplicates.iterrows():
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

                # verplaats de csv naar de old folder, enkel een timestamp geven aan de nieuwe csv
                move_csv_file(csv_path, old_csv_dir, timestamp=(folder == new_csv_dir))

                # log hoeveel nieuwe rijen er zijn toegevoegd
                print(f"Number of new (non-duplicate) rows found in {csv_path}: {new_rows_count}")

    session.close()