from .base import Base

import logging
from sqlalchemy.orm import Mapped, mapped_column, sessionmaker
from sqlalchemy import String, DateTime, Integer, Date
from repository.main import get_engine, DATA_PATH
import pandas as pd
import numpy as np
from tqdm import tqdm

BATCH_SIZE = 10_000

logger = logging.getLogger(__name__)

#data in commentaar is data die al in AfspraakContact staat, maar gejoined is. Ander duplicate data.
class AfspraakAccount(Base):
    __tablename__ = "AfspraakAccount"
    __table_args__ = {"extend_existing": True}
    AfspraakID: Mapped[str] = mapped_column(String(255), nullable=False, primary_key=True)
    #Thema: Mapped[str] = mapped_column(String(255), nullable=True)
    #Subthema: Mapped[str] = mapped_column(String(255), nullable=True)
    #Onderwerp: Mapped[str] = mapped_column(String(255), nullable=True)
    #Einddatum: Mapped[Date] = mapped_column(Date, nullable=True)
    AccountID: Mapped[str] = mapped_column(String(255), nullable=True)
    #KeyPhrases: Mapped[str] = mapped_column(String(3000), nullable=True)
    

def insert_AfspraakAccount_data(AfspraakAccount_data, session):
    session.bulk_save_objects(AfspraakAccount_data)
    session.commit()


def seed_afspraak_account():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    logger.info("Reading CSV...")
    csv = DATA_PATH + '/Afspraak_account_gelinkt_cleaned.csv'
    df = pd.read_csv(csv, delimiter=",", encoding='utf-8-sig', keep_default_na=True, na_values=[''])
    df = df.drop_duplicates()
    df = df.replace({np.nan: None})
    df["crm_Afspraak_ACCOUNT_GELINKT_Eindtijd"] = pd.to_datetime(
        df["crm_Afspraak_ACCOUNT_GELINKT_Eindtijd"], format="%d-%m-%Y"
    )
    AfspraakAccount_data = []
    logger.info("Seeding inserting rows")
    progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)
    for _, row in df.iterrows():
        aa = AfspraakAccount(
            AfspraakID=row["crm_Afspraak_ACCOUNT_GELINKT_Afspraak"],
            #Thema=row["crm_Afspraak_ACCOUNT_GELINKT_Thema"],
            #Subthema=row["crm_Afspraak_ACCOUNT_GELINKT_Subthema"],
            #Onderwerp=row["crm_Afspraak_ACCOUNT_GELINKT_Onderwerp"],
            #Einddatum=row["crm_Afspraak_ACCOUNT_GELINKT_Eindtijd"],
            AccountID=row["crm_Afspraak_ACCOUNT_GELINKT_Account"],
            #KeyPhrases=row["crm_Afspraak_ACCOUNT_GELINKT_KeyPhrases"]
        )

        AfspraakAccount_data.append(aa)
        
        if len(AfspraakAccount_data) >= BATCH_SIZE:
            insert_AfspraakAccount_data(AfspraakContact_data, session)
            AfspraakContact_data = []
            progress_bar.update(BATCH_SIZE)

    if AfspraakAccount_data:
        insert_AfspraakAccount_data(AfspraakAccount_data, session)
        progress_bar.update(len(AfspraakAccount_data))

        
        

