from .base import Base

import logging
from sqlalchemy.orm import Mapped, mapped_column, sessionmaker
from sqlalchemy import String
from repository.main import get_engine, DATA_PATH
import pandas as pd
import numpy as np
from tqdm import tqdm



BATCH_SIZE = 10_000

logger = logging.getLogger(__name__)

class AfspraakAlle(Base):
    __tablename__ = "AfspraakAlle"
    __table_args__ = {"extend_existing": True}
    AfspraakID: Mapped[str] = mapped_column(String(255), nullable=False, primary_key=True)
   

def insert_AfspraakAlle_data(AfspraakAlle_data, session):
    session.bulk_save_objects(AfspraakAlle_data)
    session.commit()


def seed_afspraak_alle():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    logger.info("Reading CSV...")
    csv = DATA_PATH + '/Afspraak_alle.csv'
    df = pd.read_csv(csv, delimiter=",", encoding='utf-8-sig', keep_default_na=True, na_values=[''])
    df = df.drop_duplicates()
    df = df.replace({np.nan: None})
    AfspraakAlle_data = []
    logger.info("Seeding inserting rows")
    progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)
    for _, row in df.iterrows():
        aa = AfspraakAlle(
            AfspraakID=row["crm_Afspraak_ALLE_Afspraak"]
        )

        AfspraakAlle_data.append(aa)
        
        if len(AfspraakAlle_data) >= BATCH_SIZE:
            AfspraakAlle_data(AfspraakContact_data, session)
            AfspraakContact_data = []
            progress_bar.update(BATCH_SIZE)

    if AfspraakAlle_data:
        insert_AfspraakAlle_data(AfspraakAlle_data, session)
        progress_bar.update(len(AfspraakAlle_data))
