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



BATCH_SIZE = 1_000

logger = logging.getLogger(__name__)

class InfoEnKlachten(Base):
    __tablename__ = "InfoEnKlachten"
    __table_args__ = {"extend_existing": True}
    Aanvraag: Mapped[str] = mapped_column(String(50), nullable=False, primary_key=True)
    Account: Mapped[str] = mapped_column(String(50), nullable=True)
    Datum: Mapped[DateTime] = mapped_column(DateTime, nullable=True)
    DatumAfsluiting: Mapped[DateTime] = mapped_column(DateTime, nullable=True)
    Status: Mapped[str] = mapped_column(String(15), nullable=False)
    Eigenaar: Mapped[str] = mapped_column(String(50), nullable=False)
    
def insert_info_en_klachten_data(info_en_klachten_data, session):
    session.bulk_save_objects(info_en_klachten_data)
    session.commit()

def seed_info_en_klachten():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    logger.info("Reading CSV...")
    # df = pd.read_csv('../Data/csv/Info en klachten.csv', delimiter=",", encoding='latin-1', keep_default_na=True, na_values=[''])
    csv = DATA_PATH + '/Info en klachten.csv'
    df = pd.read_csv(csv, delimiter=",", encoding='utf-8', keep_default_na=True, na_values=[''])
    df = df.replace({np.nan: None})
    df = df.replace({'': None})
    
    df['crm_Info_en_Klachten_Datum'] = pd.to_datetime(df['crm_Info_en_Klachten_Datum'], format='%d-%m-%Y %H:%M:%S')
    df['crm_Info_en_Klachten_Datum_afsluiting'] = pd.to_datetime(df['crm_Info_en_Klachten_Datum_afsluiting'], format='%d-%m-%Y %H:%M:%S')
    
    info_en_klachten_data = []
    logger.info("Seeding inserting rows")
    progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)

    for _, row in df.iterrows():
        p = InfoEnKlachten(                
            Aanvraag=row["crm_Info_en_Klachten_Aanvraag"],
            Account=row["crm_Info_en_Klachten_Account"],
            Datum=row["crm_Info_en_Klachten_Datum"],
            DatumAfsluiting=row["crm_Info_en_Klachten_Datum_afsluiting"],
            Status=row["crm_Info_en_Klachten_Status"],
            Eigenaar=row["crm_Info_en_Klachten_Eigenaar"]
        )
        info_en_klachten_data.append(p)
        
        if len(info_en_klachten_data) >= BATCH_SIZE:
            insert_info_en_klachten_data(info_en_klachten_data, session)
            info_en_klachten_data = []
            progress_bar.update(BATCH_SIZE)

    if info_en_klachten_data:
        insert_info_en_klachten_data(info_en_klachten_data, session)
        progress_bar.update(len(info_en_klachten_data))

