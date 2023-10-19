from .base import Base

import logging
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy import String, Integer, Date
from sqlalchemy.orm import sessionmaker
from repository.main import get_engine, DATA_PATH
import pandas as pd
import numpy as np
from tqdm import tqdm

BATCH_SIZE = 10_000

logger = logging.getLogger(__name__)

class Account(Base):
    __tablename__ = "Account"
    __table_args__ = {"extend_existing": True}
    Account: Mapped[str] = mapped_column(String(50), primary_key=True)
    AdresGeografischeRegio: Mapped[str] = mapped_column(String(50), nullable=True)
    AdresGeografischeSubregio: Mapped[str] = mapped_column(String(50), nullable=True)
    AdresPlaats: Mapped[str] = mapped_column(String(50), nullable=True)
    AdresPostcode: Mapped[str] = mapped_column(String(50), nullable=True) # sommige buitenlandse postcodes bevatten letters
    AdresProvincie: Mapped[str] = mapped_column(String(50), nullable=True)
    IndustriezoneNaam: Mapped[str] = mapped_column(String(250), nullable=True) # erg lange namen zoals 'OV - (9051) The Loop - Poortakkerstraat - Flanders Expo'
    IsVokaEntiteit: Mapped[str] = mapped_column(String(50))
    Ondernemingsaard: Mapped[str] = mapped_column(String(50), nullable=True)
    Ondernemingstype: Mapped[str] = mapped_column(String(50), nullable=True)
    Oprichtingsdatum: Mapped[Date] = mapped_column(Date)
    PrimaireActiviteit: Mapped[str] = mapped_column(String(50), nullable=True)
    RedenVanStatus: Mapped[str] = mapped_column(String(50))
    Status: Mapped[str] = mapped_column(String(50))
    VokaNr: Mapped[int] = mapped_column(Integer)
    HoofdNaCeCode: Mapped[str] = mapped_column(String(50), nullable=True)
    AdresLand: Mapped[str] = mapped_column(String(50), nullable=True)


def insert_account_data(account_data, session):
    session.bulk_save_objects(account_data)
    session.commit()
    

def seed_account():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    logger.info("Reading CSV...")
    csv = DATA_PATH + "/Account.csv"
    df = pd.read_csv(csv, delimiter=",", encoding="utf-8", keep_default_na=True, na_values=[""])
    df = df.replace({np.nan: None})
    df = df.replace({"": None})
    
    df["crm_Account_Oprichtingsdatum"] = pd.to_datetime(df["crm_Account_Oprichtingsdatum"], format="%d-%m-%Y")

    account_data = []
    logger.info("Seeding inserting rows")
    progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)

    for _, row in df.iterrows():
        p = Account(
                Account=row["crm_Account_Account"],
                AdresGeografischeRegio=row["crm_Account_Adres_Geografische_regio"],
                AdresGeografischeSubregio=row["crm_Account_Adres_Geografische_subregio"],
                AdresPlaats=row["crm_Account_Adres_Plaats"],
                AdresPostcode=row["crm_Account_Adres_Postcode"],
                AdresProvincie=row["crm_Account_Adres_Provincie"],
                IndustriezoneNaam=row["crm_Account_Industriezone_Naam_"],
                IsVokaEntiteit=row["crm_Account_Is_Voka_entiteit"],
                Ondernemingsaard=row["crm_Account_Ondernemingsaard"],
                Ondernemingstype=row["crm_Account_Ondernemingstype"],
                Oprichtingsdatum=row["crm_Account_Oprichtingsdatum"],
                PrimaireActiviteit=row["crm_Account_Primaire_activiteit"],
                RedenVanStatus=row["crm_Account_Reden_van_status"],
                Status=row["crm_Account_Status"],
                VokaNr=row["crm_Account_Voka_Nr_"],
                HoofdNaCeCode=row["crm_Account_Hoofd_NaCe_Code"],
                AdresLand=row["crm_Account_Adres_Land"]
        )
        account_data.append(p)

        if len(account_data) >= BATCH_SIZE:
            insert_account_data(account_data, session)
            account_data = []
            progress_bar.update(BATCH_SIZE)

    if account_data:
        insert_account_data(account_data, session)
        progress_bar.update(len(account_data))
