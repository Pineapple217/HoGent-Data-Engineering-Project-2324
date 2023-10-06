from .base import Base

import logging
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy import String, DateTime, Numeric, Integer, Date
from sqlalchemy.orm import sessionmaker
from repository import get_engine
import pandas as pd
import numpy as np
from tqdm import tqdm
import concurrent.futures

NUM_THREADS = 2
BATCH_SIZE = 10_000
logger = logging.getLogger(__name__)


class SessieInschrijving(Base):
    __tablename__ = "sessie_inschrijving"  # snakecase
    __table_args__ = {"extend_existing": True}
    Sessie_Inschrijving: Mapped[str] = mapped_column(
        String(50), nullable=False, primary_key=True
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
    df = pd.read_csv(
        "../Data/Sessie inschrijving.csv",
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
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = []
        for i, row in df.iterrows():
            p = SessieInschrijving(
                SessieInschrijving=row["crm_SessieInschrijving_SessieInschrijving"],
                Sessie=row["crm_SessieInschrijving_Sessie"],
                Inschrijving=row["crm_SessieInschrijving_Inschrijving"],
            )
            sessie_inschrijving_data.append(p)

            if len(sessie_inschrijving_data) >= BATCH_SIZE:
                futures.append(executor.submit(insert_sessie_inschrijving_data, sessie_inschrijving_data, session))
                sessie_inschrijving_data = []
                progress_bar.update(BATCH_SIZE)

        # Insert any remaining data
        if sessie_inschrijving_data:
            futures.append(executor.submit(insert_sessie_inschrijving_data, sessie_inschrijving_data, session))

        concurrent.futures.wait(futures)
