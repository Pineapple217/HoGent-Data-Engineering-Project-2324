from .base import Base

import logging
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy import String, Integer
from sqlalchemy.orm import sessionmaker
from repository.main import get_engine, DATA_PATH
import pandas as pd
import numpy as np
from tqdm import tqdm


BATCH_SIZE = 10_000

logger = logging.getLogger(__name__)

# CSV MOET OPGEKUIST WORDEN, LEGE RIJEN MET , WEG

class ContactficheFunctie(Base):
    __tablename__ = "ContactficheFunctie" 
    __table_args__ = {"extend_existing": True}
    Id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    Contactpersoon: Mapped[str] = mapped_column(String(50))
    Functie: Mapped[str] = mapped_column(String(50))


def insert_contactfiche_functie_data(contactfiche_functie_data, session):
    session.bulk_save_objects(contactfiche_functie_data)
    session.commit()

def seed_contactfiche_functie():
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
        skiprows=[1, 2506]
    )
    df = df.dropna(how='all', axis=0)
    df = df.replace({np.nan: None})
    contactfiche_functie_data = []
    logger.info("Seeding inserting rows")
    progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)
    
    for i, row in df.iterrows():
        p = ContactficheFunctie(
            Contactpersoon=row["crm_ContactFunctie_Contactpersoon"],
            Functie=row["crm_ContactFunctie_Functie"],
        )
        contactfiche_functie_data.append(p)

        if len(contactfiche_functie_data) >= BATCH_SIZE:
            insert_contactfiche_functie_data(contactfiche_functie_data, session)
            contactfiche_functie_data = []
            progress_bar.update(BATCH_SIZE)

    if contactfiche_functie_data:
        insert_contactfiche_functie_data(contactfiche_functie_data, session)
        progress_bar.update(len(contactfiche_functie_data))
