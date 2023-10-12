from .base import Base

import logging
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy import String, DateTime, Numeric, Integer, Date, Float
from sqlalchemy.dialects.mssql import DATETIME2
from sqlalchemy.orm import sessionmaker
from repository.main import get_engine, DATA_PATH
import pandas as pd
import numpy as np
from tqdm import tqdm
import concurrent.futures


BATCH_SIZE = 10
logger = logging.getLogger(__name__)


class Functie(Base):
    __tablename__ = "Functie"  # snakecase
    __table_args__ = {"extend_existing": True}
    Functie: Mapped[str] = mapped_column(String(50), nullable=False, primary_key=True)
    Naam: Mapped[str] = mapped_column(String(75), nullable=True)


def insert_functie_data(functie_data, session):
    session.bulk_save_objects(functie_data)
    session.commit()


def seed_functie():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    logger.info("Reading CSV...")
    csv = DATA_PATH + "/Functie.csv"
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
    functie_data = []
    logger.info("Seeding inserting rows")
    progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)
    for i, row in df.iterrows():
        p = Functie(
            Functie=row["crm_Functie_Functie"],
            Naam=row["crm_Functie_Naam"],
        )

        functie_data.append(p)

        if len(functie_data) >= BATCH_SIZE:
            insert_functie_data(functie_data, session)
            functie_data = []
            progress_bar.update(BATCH_SIZE)

    # Insert any remaining data
    if functie_data:
        insert_functie_data(functie_data, session)
        progress_bar.update(len(functie_data))
