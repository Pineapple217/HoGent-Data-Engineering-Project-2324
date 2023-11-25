from .base import Base
from .functionalities import load_csv, move_csv_file

import logging
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column, relationship
from sqlalchemy import String, ForeignKey, text
from sqlalchemy.dialects.mssql import DATETIME2
from sqlalchemy.orm import sessionmaker
from repository.main import get_engine, DATA_PATH
import os
import pandas as pd
import numpy as np
from tqdm import tqdm
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from .campagne import Campagne
    from .sessie_inschrijving import SessieInschrijving

BATCH_SIZE = 10_000
DATE_FORMAT = "%d-%m-%Y %H:%M:%S"

logger = logging.getLogger(__name__)


class Sessie(Base):
    __tablename__ = "Sessie"
    __table_args__ = {"extend_existing": True}
    SessieId: Mapped[str] = mapped_column(String(50), nullable=True, primary_key=True)
    Activiteitstype: Mapped[str] = mapped_column(String(50), nullable=True)
    EindDatumTijd: Mapped[DATETIME2] = mapped_column(DATETIME2, nullable=True)
    Product: Mapped[str] = mapped_column(String(50), nullable=True)
    SessieNr: Mapped[str] = mapped_column(String(50), nullable=True)
    StartDatumTijd: Mapped[DATETIME2] = mapped_column(DATETIME2, nullable=True)
    ThemaNaam: Mapped[str] = mapped_column(String(50), nullable=True)

    # FK
    CampagneId: Mapped[Optional[str]] = mapped_column(ForeignKey("Campagne.CampagneId", use_alter=True), nullable=True)
    Campagne: Mapped["Campagne"] = relationship(back_populates="Sessie")

    SessieInschrijving: Mapped["SessieInschrijving"] = relationship(back_populates="Sessie")


def insert_sessie_data(sessie_data, session):
    session.bulk_save_objects(sessie_data)
    session.commit()

def get_existing_ids(session):
    return [result[0] for result in session.query(Sessie.SessieId).all()]

def seed_sessie():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    existing_ids = get_existing_ids(session)

    old_csv_dir = os.path.join(DATA_PATH, "old")
    new_csv_dir = os.path.join(DATA_PATH, "new")
    if not os.path.exists(old_csv_dir) or not os.path.exists(new_csv_dir):
        raise FileNotFoundError("The folders 'old' and 'new' must exist in the data folder")
    
    folder_new = new_csv_dir

    sessie_data = []
    for filename in os.listdir(folder_new): #check alle filenames in 'new'
        if filename == 'Sessie.csv':
            csv_path = os.path.join(folder_new, filename)
    
            logger.info(f"Reading CSV: {csv_path}")
            df, error = load_csv(csv_path)
            if error:
                raise Exception(f"Error loading CSV: {csv_path, error}")
            
            df = df.replace({np.nan: None})

            df = df[~df['crm_Sessie_Sessie'].isin(existing_ids)]  

            df["crm_Sessie_Eind_Datum_Tijd"] = pd.to_datetime(df["crm_Sessie_Eind_Datum_Tijd"], format=DATE_FORMAT)
            df["crm_Sessie_Start_Datum_Tijd"] = pd.to_datetime(df["crm_Sessie_Start_Datum_Tijd"], format=DATE_FORMAT)

            # data in chunks steken
            chunks = [df[i:i + BATCH_SIZE] for i in range(0, df.shape[0], BATCH_SIZE)]

            progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)

            for chunk in chunks:
                sessie_data = []
                for _, row in chunk.iterrows():
                    p = Sessie(
                        SessieId=row["crm_Sessie_Sessie"],
                        Activiteitstype=row["crm_Sessie_Activiteitstype"],
                        CampagneId=row["crm_Sessie_Campagne"],
                        EindDatumTijd=row["crm_Sessie_Eind_Datum_Tijd"],
                        Product=row["crm_Sessie_Product"],
                        SessieNr=row["crm_Sessie_Sessie_nr_"],
                        StartDatumTijd=row["crm_Sessie_Start_Datum_Tijd"],
                        ThemaNaam=row["crm_Sessie_Thema_Naam_"],
                    )
                    sessie_data.append(p)

                insert_sessie_data(sessie_data, session)
                progress_bar.update(len(sessie_data))

            progress_bar.close()

            move_csv_file(csv_path, old_csv_dir)

            logger.info(f"Number of new (non-duplicate) rows found in {csv_path}: {len(df)}")

    if not sessie_data:
        logger.info("No new data was given. Data is up to date already.")

    session.execute(
        text(
            """
        UPDATE Sessie
        SET Sessie.CampagneId = NULL
        WHERE Sessie.CampagneId
        NOT IN
        (SELECT CampagneId FROM Campagne)
    """
        )
    )
    session.commit()
