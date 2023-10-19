from .base import Base

import logging
from sqlalchemy.orm import Mapped, mapped_column, sessionmaker, relationship
from sqlalchemy import String, ForeignKey
from sqlalchemy.dialects.mssql import DATETIME2
from repository.main import get_engine, DATA_PATH
import pandas as pd
import numpy as np
from tqdm import tqdm
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .gebruiker import Gebruiker
    
BATCH_SIZE = 10_000

logger = logging.getLogger(__name__)

class InfoEnKlachten(Base):
    __tablename__ = "InfoEnKlachten"
    __table_args__ = {"extend_existing": True}
    Aanvraag: Mapped[str] = mapped_column(String(50), primary_key=True)
    Account: Mapped[str] = mapped_column(String(50))
    Datum: Mapped[DATETIME2] = mapped_column(DATETIME2)
    DatumAfsluiting: Mapped[DATETIME2] = mapped_column(DATETIME2)
    Status: Mapped[str] = mapped_column(String(15))
    Eigenaar: Mapped[str] = mapped_column(String(50), ForeignKey("Gebruiker.Id"), nullable=True)
    
    parent: Mapped["Gebruiker"] = relationship(back_populates="children")


def insert_info_en_klachten_data(info_en_klachten_data, session):
    session.bulk_save_objects(info_en_klachten_data)
    session.commit()


def seed_info_en_klachten():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    logger.info("Reading CSV...")
    csv = DATA_PATH + "/Info en klachten.csv"
    
    csv_Gebruiker = DATA_PATH + '/Gebruikers.csv'
    gebruiker_df = pd.read_csv(csv_Gebruiker, delimiter=",", encoding='utf-8-sig', keep_default_na=True, na_values=[''])
    valid_gebruikers = gebruiker_df["crm_Gebruikers_CRM_User_ID"].tolist()
    
    df = pd.read_csv(csv, delimiter=",", encoding="utf-8", keep_default_na=True, na_values=[""])
    df = df.replace({np.nan: None})
    df = df.replace({"": None})
    df = df.drop_duplicates(subset=['crm_Info_en_Klachten_Aanvraag']) # duplicaten van primary keys in de csv
    
    df["crm_Info_en_Klachten_Datum"] = pd.to_datetime(df["crm_Info_en_Klachten_Datum"], format="%d-%m-%Y %H:%M:%S")
    df["crm_Info_en_Klachten_Datum_afsluiting"] = pd.to_datetime(df["crm_Info_en_Klachten_Datum_afsluiting"], format="%d-%m-%Y %H:%M:%S")

    info_en_klachten_data = []
    logger.info("Seeding inserting rows")
    progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)

    for _, row in df.iterrows():
        eigenaar=row["crm_Info_en_Klachten_Eigenaar"]
        p = InfoEnKlachten(
            Aanvraag=row["crm_Info_en_Klachten_Aanvraag"],
            Account=row["crm_Info_en_Klachten_Account"],
            Datum=row["crm_Info_en_Klachten_Datum"],
            DatumAfsluiting=row["crm_Info_en_Klachten_Datum_afsluiting"],
            Status=row["crm_Info_en_Klachten_Status"],
            Eigenaar=eigenaar,
        )
        if eigenaar not in valid_gebruikers:
            p.Eigenaar=None 
        info_en_klachten_data.append(p)

        if len(info_en_klachten_data) >= BATCH_SIZE:
            insert_info_en_klachten_data(info_en_klachten_data, session)
            info_en_klachten_data = []
            progress_bar.update(BATCH_SIZE)
        

    if info_en_klachten_data:
        insert_info_en_klachten_data(info_en_klachten_data, session)
        progress_bar.update(len(info_en_klachten_data))
