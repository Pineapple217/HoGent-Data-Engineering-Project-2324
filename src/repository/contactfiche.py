from .base import Base

import logging
from sqlalchemy.orm import Mapped, mapped_column, sessionmaker
from sqlalchemy import String, Integer
from sqlalchemy.dialects.mssql import BIT
from repository.main import get_engine, DATA_PATH
import pandas as pd
import numpy as np
from tqdm import tqdm

BATCH_SIZE = 10_000

logger = logging.getLogger(__name__)

class Contactfiche(Base):
    __tablename__ = "Contactfiche" 
    __table_args__ = {"extend_existing": True}
    Id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)  # Id aanmaken want primary key is niet te vinden
    ContactPersoon: Mapped[str] = mapped_column(String(50))
    Account: Mapped[str] = mapped_column(String(50))
    FunctieTitel: Mapped[str] = mapped_column(String(255), nullable=True)
    PersoonId: Mapped[str] = mapped_column(String(50))
    Status: Mapped[str] = mapped_column(String(50))
    VokaMedewerker: Mapped[BIT] = mapped_column(BIT)


def insert_contactfiche_data(contactfiche_data, session):
    session.bulk_save_objects(contactfiche_data)
    session.commit()


def seed_contactfiche():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    logger.info("Reading CSV...")
    csv = DATA_PATH + "/Contact.csv"
    df = pd.read_csv(
        csv,
        delimiter=",",
        encoding="utf-8",
        keep_default_na=True,
        na_values=[""],
    )
    df = df.replace({np.nan: None})
    contactfiche_data = []
    logger.info("Seeding inserting rows")
    progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)
    
    for i, row in df.iterrows():
        p = Contactfiche(
            ContactPersoon=row["crm_Contact_Contactpersoon"],
            Account=row["crm_Contact_Account"],
            FunctieTitel=row["crm_Contact_Functietitel"],
            PersoonId=row["crm_Contact_Persoon_ID"],
            Status=row["crm_Contact_Status"],
            VokaMedewerker=row["crm_Contact_Voka_medewerker"],
        )
        contactfiche_data.append(p)

        if len(contactfiche_data) >= BATCH_SIZE:
            insert_contactfiche_data(contactfiche_data, session)
            contactfiche_data = []
            progress_bar.update(BATCH_SIZE)

    if contactfiche_data:
        insert_contactfiche_data(contactfiche_data, session)
        progress_bar.update(len(contactfiche_data))
