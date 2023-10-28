from .base import Base

import logging
from sqlalchemy.orm import Mapped, mapped_column, sessionmaker, relationship
from sqlalchemy import String
from repository.main import get_engine, DATA_PATH
import pandas as pd
import numpy as np
from tqdm import tqdm

BATCH_SIZE = 10

logger = logging.getLogger(__name__)


class Functie(Base):
    __tablename__ = "Functie"
    __table_args__ = {"extend_existing": True}
    FunctieId: Mapped[str] = mapped_column(String(50), primary_key=True)
    Naam: Mapped[str] = mapped_column(String(75))
    
    # FK
    ContactficheFunctie: Mapped["ContactficheFunctie"] = relationship(back_populates="Functie")


def insert_functie_data(functie_data, session):
    session.bulk_save_objects(functie_data)
    session.commit()


def seed_functie():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    
    logger.info("Reading CSV...")
    csv = DATA_PATH + "/Functie.csv"
    df = pd.read_csv(csv, delimiter=",", encoding="utf-8", keep_default_na=True, na_values=[""])
    
    df = df.replace({np.nan: None})
    
    functie_data = []
    logger.info("Seeding inserting rows")
    progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)
    for _, row in df.iterrows():
        p = Functie(
            FunctieId=row["crm_Functie_Functie"],
            Naam=row["crm_Functie_Naam"],
        )
        functie_data.append(p)

        if len(functie_data) >= BATCH_SIZE:
            insert_functie_data(functie_data, session)
            functie_data = []
            progress_bar.update(BATCH_SIZE)

    if functie_data:
        insert_functie_data(functie_data, session)
        progress_bar.update(len(functie_data))