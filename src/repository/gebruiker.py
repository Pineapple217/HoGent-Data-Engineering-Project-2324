from .base import Base

import logging
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy import String, DateTime, Integer, Date
from sqlalchemy.orm import sessionmaker
from repository.main import get_engine, DATA_PATH
import pandas as pd
import numpy as np
from tqdm import tqdm
import concurrent.futures
import os

NUM_THREADS = 2
BATCH_SIZE = 10_000

logger = logging.getLogger(__name__)

class Gebruiker(Base):
    __tablename__ = "Gebruiker"
    __table_args__ = {"extend_existing": True}
    Id: Mapped[str] = mapped_column(String(50), nullable=False, primary_key=True)
    BusinessUnitNaam: Mapped[str] = mapped_column(String(50), nullable=True)

def insert_gebruiker_data(gebruiker_data, session):
    session.bulk_save_objects(gebruiker_data)
    session.commit()

def seed_gebruiker():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    logger.info("Reading CSV...")
    # df = pd.read_csv('../Data/csv/Gebruikers.csv', delimiter=",", encoding='latin-1', keep_default_na=True, na_values=[''])
    csv = DATA_PATH + '/Gebruikers.csv'
    df = pd.read_csv(csv, delimiter=",", encoding='utf-8-sig', keep_default_na=True, na_values=[''])
    df = df.replace({np.nan: None})
    gebruiker_data = []
    logger.info("Seeding inserting rows")
    progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = []
        for _, row in df.iterrows():
            # print(row)
            p = Gebruiker(
                Id=row["crm_Gebruikers_CRM_User_ID"],
                BusinessUnitNaam=row["crm_Gebruikers_Business_Unit_Naam_"]
            )
            gebruiker_data.append(p)
            
            if len(gebruiker_data) >= BATCH_SIZE:
                futures.append(executor.submit(insert_gebruiker_data, gebruiker_data, session))
                gebruiker_data = []
                progress_bar.update(BATCH_SIZE)

        # Insert any remaining data
        if gebruiker_data:
            futures.append(executor.submit(insert_gebruiker_data, gebruiker_data, session))

        concurrent.futures.wait(futures)

# lokaal getest