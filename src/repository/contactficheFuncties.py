from .base import Base

import logging
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy import String, DateTime, Numeric, Integer, Date, Float
from sqlalchemy.dialects.mssql import DATETIME2
from sqlalchemy.orm import sessionmaker
from repository.main import get_engine, DATA_PATH
import pandas as pd
import numpy as np
from tqdm import tqdm
import concurrent.futures


BATCH_SIZE = 10_000
logger = logging.getLogger(__name__)


class ContactficheFunctie(Base):
    __tablename__ = "ContactficheFuncties"  # snakecase
    __table_args__ = {"extend_existing": True}
    Id: Mapped[int] = mapped_column(
        Integer, nullable=False, primary_key=True, autoincrement=True
    )
    Contactpersoon: Mapped[str] = mapped_column(String(50), nullable=False)
    Functie: Mapped[str] = mapped_column(String(50), nullable=True)


def insert_contactFunctie_data(contactFunctie_data, session):
    session.bulk_save_objects(contactFunctie_data)
    session.commit()


def seed_contactFunctie():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    logger.info("Reading CSV...")
    csv = DATA_PATH + "/Contact functie.csv"
    df = pd.read_csv(
        csv,
        delimiter=",",
        encoding="utf-8",
        keep_default_na=True,
        na_values=[""],
    )
    df = df.replace({np.nan: None})
    # Sommige lege waardes worden als NaN ingelezeno
    # NaN mag niet in een varchar
    contactFunctie_data = []
    logger.info("Seeding inserting rows")
    progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)
    for i, row in df.iterrows():
        p = ContactficheFunctie(
            Contactpersoon=row["crm_ContactFunctie_Contactpersoon"],
            Functie=row["crm_ContactFunctie_Functie"],
        )

        contactFunctie_data.append(p)

        if len(contactFunctie_data) >= BATCH_SIZE:
            insert_contactFunctie_data(contactFunctie_data, session)
            contactFunctie_data = []
            progress_bar.update(BATCH_SIZE)

    # Insert any remaining data
    if contactFunctie_data:
        insert_contactFunctie_data(contactFunctie_data, session)
        progress_bar.update(len(contactFunctie_data))
