from .base import Base
from .functionalities import load_csv, move_csv_file

import logging
from sqlalchemy.orm import Mapped, mapped_column, sessionmaker, relationship
from sqlalchemy import String, Integer, Date
from repository.main import get_engine, DATA_PATH
import os
import pandas as pd
import numpy as np
from tqdm import tqdm
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .contactfiche import Contactfiche
    from .info_en_klachten import InfoEnKlachten
    from .account_financiele_data import AccountFinancieleData
    from .afspraak_account import AfspraakAccount
    from .account_activiteitscode import AccountActiviteitscode

BATCH_SIZE = 10_000

logger = logging.getLogger(__name__)


class Account(Base):
    __tablename__ = "Account"
    __table_args__ = {"extend_existing": True}
    AccountId: Mapped[str] = mapped_column(String(50), primary_key=True)
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
    
    # FK
    InfoEnKlachten: Mapped["InfoEnKlachten"] = relationship(back_populates="Account")
    Contactfiche: Mapped["Contactfiche"] = relationship(back_populates="Account")
    AccountFinancieleData: Mapped["AccountFinancieleData"] = relationship(back_populates="Onderneming")
    AfspraakAccount: Mapped["AfspraakAccount"] = relationship(back_populates="Account")
    AccountActiviteitscode: Mapped["AccountActiviteitscode"] = relationship(back_populates="Account")


def insert_account_data(account_data, session):
    session.bulk_save_objects(account_data)
    session.commit()


#functie om alle id's te querien, zodat gelijke rijden niet appended worden
def get_existing_ids(session):
    return [result[0] for result in session.query(Account.AccountId).all()]
    

def seed_account():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    #query bestaande id's
    existing_ids = get_existing_ids(session)
    
    #stel dirs voor old en new in en check of ze kloppen
    old_csv_dir = os.path.join(DATA_PATH, "old")
    new_csv_dir = os.path.join(DATA_PATH, "new")
    if not os.path.exists(old_csv_dir) or not os.path.exists(new_csv_dir):
        raise FileNotFoundError("The folders 'old' and 'new' must exist in the data folder")
    
    folder_new = new_csv_dir

    account_data = []
    for filename in os.listdir(folder_new): #check alle filenames in 'new'
        if filename == 'Account.csv': #hardcoded filename, zodat startswith niet fout kan lopen
            csv_path = os.path.join(folder_new, filename) #vul filepath aan met gevonden file
    
            logger.info(f"Reading CSV: {csv_path}")
            df, error = load_csv(csv_path) #load_csv uit functionalities.py, probeert met hardcoded delimiters en encodings een df te maken
            if error:
                raise Exception(f"Error loading CSV: {csv_path, error}")
            
            df = df.replace({np.nan: None})
            df = df.replace({"": None})
            
            df["crm_Account_Oprichtingsdatum"] = pd.to_datetime(df["crm_Account_Oprichtingsdatum"], format="%d-%m-%Y")

            #filter df op alle accounts die nog niet(~) bestaan, voor iterrows
            df = df[~df['crm_Account_Account'].isin(existing_ids)]  

            # data in chunks steken
            chunks = [df[i:i + BATCH_SIZE] for i in range(0, df.shape[0], BATCH_SIZE)]

            progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)

            for chunk in chunks:
                account_data = []
                for _, row in chunk.iterrows():
                    p = Account(
                            AccountId=row["crm_Account_Account"],
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

                insert_account_data(account_data, session)
                progress_bar.update(len(account_data))
                
            progress_bar.close()

            move_csv_file(csv_path, old_csv_dir)

            logger.info(f"Number of new (non-duplicate) rows found in {csv_path}: {len(df)}")

    if not account_data:
        logger.info("No new data was given. Data is up to date already.")

