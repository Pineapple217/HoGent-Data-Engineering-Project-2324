from .base import Base

import logging
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy import String, DateTime, Numeric, Integer, Date
from sqlalchemy.orm import sessionmaker
from repository.main import get_engine, DATA_PATH
import pandas as pd
import numpy as np
from tqdm import tqdm
import concurrent.futures


BATCH_SIZE = 10_000
logger = logging.getLogger(__name__)


class SessieInschrijving(Base):
    __tablename__ = "SessieInschrijving"  # snakecase
    __table_args__ = {"extend_existing": True}
    SessieInschrijving: Mapped[str] = mapped_column(
        String(50), nullable=True, primary_key=True
    )
    Sessie: Mapped[str] = mapped_column(String(50), nullable=True)
    Inschrijving: Mapped[str] = mapped_column(String(50), nullable=True)

def insert_sessie_inschrijving_data(sessie_inschrijving_data, session):
    session.bulk_save_objects(sessie_inschrijving_data)
    session.commit()


def seed_sessie_inschrijving():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    logger.info("Reading CSV...")
    csv = DATA_PATH + '/Sessie inschrijving.csv'
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
    sessie_inschrijving_data = []

    logger.info("Seeding inserting rows")
    progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)

    for i, row in df.iterrows():
        p = SessieInschrijving(
            SessieInschrijving=row["crm_SessieInschrijving_SessieInschrijving"],
            Sessie=row["crm_SessieInschrijving_Sessie"],
            Inschrijving=row["crm_SessieInschrijving_Inschrijving"],
        )
        sessie_inschrijving_data.append(p)

        if len(sessie_inschrijving_data) >= BATCH_SIZE:
            insert_sessie_inschrijving_data(sessie_inschrijving_data, session)
            sessie_inschrijving_data = []
            progress_bar.update(BATCH_SIZE)

    # Insert any remaining data
    if sessie_inschrijving_data:
        insert_sessie_inschrijving_data(sessie_inschrijving_data, session)
        progress_bar.update(len(sessie_inschrijving_data))


