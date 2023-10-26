from .base import Base

import logging
from sqlalchemy.orm import Mapped, mapped_column, sessionmaker, relationship
from sqlalchemy import String, ForeignKey, text
from sqlalchemy.dialects.mssql import DATETIME2
from repository.main import get_engine, DATA_PATH
import pandas as pd
import numpy as np
from tqdm import tqdm
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from .gebruiker import Gebruiker
    from .account import Account
    
BATCH_SIZE = 10_000

logger = logging.getLogger(__name__)

class InfoEnKlachten(Base):
    __tablename__ = "InfoEnKlachten"
    __table_args__ = {"extend_existing": True}
    Aanvraag: Mapped[str] = mapped_column(String(50), primary_key=True)
    Datum: Mapped[DATETIME2] = mapped_column(DATETIME2)
    DatumAfsluiting: Mapped[DATETIME2] = mapped_column(DATETIME2)
    Status: Mapped[str] = mapped_column(String(15))
    
    # FK
    EigenaarId: Mapped[Optional[str]] = mapped_column(ForeignKey("Gebruiker.Id", use_alter=True), nullable=True)
    Eigenaar: Mapped["Gebruiker"] = relationship(back_populates="InfoEnKlachten")
    
    AccountId: Mapped[Optional[str]] = mapped_column(ForeignKey("Account.Account", use_alter=True), nullable=True)
    Account: Mapped["Account"] = relationship(back_populates="InfoEnKlachten")


def insert_info_en_klachten_data(info_en_klachten_data, session):
    session.bulk_save_objects(info_en_klachten_data)
    session.commit()


def seed_info_en_klachten():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    
    logger.info("Reading CSV...")
    csv = DATA_PATH + "/Info en klachten.csv"
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
        p = InfoEnKlachten(
            Aanvraag=row["crm_Info_en_Klachten_Aanvraag"],
            AccountId=row["crm_Info_en_Klachten_Account"],
            Datum=row["crm_Info_en_Klachten_Datum"],
            DatumAfsluiting=row["crm_Info_en_Klachten_Datum_afsluiting"],
            Status=row["crm_Info_en_Klachten_Status"],
            EigenaarId=row["crm_Info_en_Klachten_Eigenaar"],
        )
        info_en_klachten_data.append(p)

        if len(info_en_klachten_data) >= BATCH_SIZE:
            insert_info_en_klachten_data(info_en_klachten_data, session)
            info_en_klachten_data = []
            progress_bar.update(BATCH_SIZE)
        
    if info_en_klachten_data:
        insert_info_en_klachten_data(info_en_klachten_data, session)
        progress_bar.update(len(info_en_klachten_data))
    
    session.execute(text("""
        UPDATE InfoEnKlachten
        SET InfoEnKlachten.EigenaarId = NULL
        WHERE InfoEnKlachten.EigenaarId
        NOT IN
        (SELECT Id FROM Gebruiker)
    """))
    session.commit()

    session.execute(text("""
        UPDATE InfoEnKlachten
        SET InfoEnKlachten.AccountId = NULL
        WHERE InfoEnKlachten.AccountId
        NOT IN
        (SELECT Account FROM Account)
    """))
    session.commit()
