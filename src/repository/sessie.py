from .base import Base

import logging
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy import String, Numeric, Integer, Date
from sqlalchemy.dialects.mssql import DATETIME2
from sqlalchemy.orm import sessionmaker
from repository.main import get_engine, DATA_PATH
import pandas as pd
import numpy as np
from tqdm import tqdm
import concurrent.futures


BATCH_SIZE = 10_000

logger = logging.getLogger(__name__)


class Sessie(Base):
    __tablename__ = "sessie"  # snakecase
    __table_args__ = {"extend_existing": True}
    Sessie: Mapped[str] = mapped_column(String(50), nullable=True, primary_key=True)
    Activiteitstype: Mapped[str] = mapped_column(String(50), nullable=True)
    Campagne: Mapped[str] = mapped_column(String(50), nullable=True)
    EindDatumTijd: Mapped[DATETIME2] = mapped_column(DATETIME2, nullable=True)
    Product: Mapped[str] = mapped_column(String(50), nullable=True)
    SessieNr: Mapped[str] = mapped_column(String(50), nullable=True)
    StartDatumTijd: Mapped[DATETIME2] = mapped_column(DATETIME2, nullable=True)
    ThemaNaam: Mapped[str] = mapped_column(String(50), nullable=True)


def insert_sessie_data(sessie_data, session):
    session.bulk_save_objects(sessie_data)
    session.commit()


def seed_sessie():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    logger.info("Reading CSV...")
    csv = DATA_PATH + "/Sessie.csv"
    df = pd.read_csv(
        csv,
        delimiter=",",
        encoding="utf-8",
        keep_default_na=True,
        na_values=[""],
    )
    df = df.replace({np.nan: None})
    # Sommige lege waardes worden als NaN ingelezeno
    # NaN mag niet in een varchar
    df["crm_Sessie_Eind_Datum_Tijd"] = pd.to_datetime(
        df["crm_Sessie_Eind_Datum_Tijd"], format="%d-%m-%Y %H:%M:%S"
    )
    df["crm_Sessie_Start_Datum_Tijd"] = pd.to_datetime(
        df["crm_Sessie_Start_Datum_Tijd"], format="%d-%m-%Y %H:%M:%S"
    )
    sessie_data = []
    logger.info("Seeding inserting rows")
    progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)

    for i, row in df.iterrows():
        p = Sessie(
            Sessie=row["crm_Sessie_Sessie"],
            Activiteitstype=row["crm_Sessie_Activiteitstype"],
            Campagne=row["crm_Sessie_Campagne"],
            EindDatumTijd=row["crm_Sessie_Eind_Datum_Tijd"],
            Product=row["crm_Sessie_Product"],
            SessieNr=row["crm_Sessie_Sessie_nr_"],
            StartDatumTijd=row["crm_Sessie_Start_Datum_Tijd"],
            ThemaNaam=row["crm_Sessie_Thema_Naam_"],
        )

        sessie_data.append(p)

        if len(sessie_data) >= BATCH_SIZE:
            insert_sessie_data(sessie_data, session)
            sessie_data = []
            progress_bar.update(BATCH_SIZE)

    # Insert any remaining data
    if sessie_data:
        insert_sessie_data(sessie_data, session)
        progress_bar.update(len(sessie_data))
        i
