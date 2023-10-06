from .base import Base

import logging
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy import String, DateTime, Numeric, Integer, Date
from sqlalchemy.orm import sessionmaker
from repository.main import get_engine, DATA_PATH
import pandas as pd
import numpy as np
from tqdm import tqdm
import concurrent.futures

NUM_THREADS = 2
BATCH_SIZE = 10_000
logger = logging.getLogger(__name__)


class Inschrijving(Base):
    __tablename__ = "inschrijving"  # snakecase
    __table_args__ = {"extend_existing": True}
    Inschrijving: Mapped[str] = mapped_column(
        String(50), nullable=False, primary_key=True
    )
    Aanwezig_Afwezig: Mapped[str] = mapped_column(String(50), nullable=True)
    Bron: Mapped[str] = mapped_column(String(20), nullable=True)
    Contactfiche: Mapped[str] = mapped_column(String(50), nullable=True)
    Datum_Inschrijving: Mapped[Date] = mapped_column(Date, nullable=True)
    Facturatie_Bedrag: Mapped[int] = mapped_column(Integer, nullable=True)

def insert_inschrijving_data(inschrijving_data, session):
    session.bulk_save_objects(inschrijving_data)
    session.commit()


def seed_inschrijving():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    logger.info("Reading CSV...")
    csv = DATA_PATH + '/Inschrijving.csv'
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
    df["crm_Inschrijving_Datum_inschrijving"] = pd.to_datetime(
        df["crm_Inschrijving_Datum_inschrijving"], format="%d-%m-%Y"
    )
    inschrijving_data = []
    logger.info("Seeding inserting rows")
    progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = []
        for i, row in df.iterrows():
            p = Inschrijving(
                Inschrijving=row["crm_Inschrijving_Inschrijving"],
                AanwezigAfwezig=row["crm_Inschrijving_Aanwezig_Afwezig"],
                Bron=row["crm_Inschrijving_Bron"],
                Contactfiche=row["crm_Inschrijving_Contactfiche"],
                DatumInschrijving=row["crm_Inschrijving_Datum_inschrijving"],
                FacturatieBedrag=row["crm_Inschrijving_Facturatie_Bedrag"],
            )

            inschrijving_data.append(p)

            if len(inschrijving_data) >= BATCH_SIZE:
                futures.append(executor.submit(insert_inschrijving_data, inschrijving_data, session))
                inschrijving_data = []
                progress_bar.update(BATCH_SIZE)


        # Insert any remaining data
        if inschrijving_data:
            futures.append(executor.submit(insert_inschrijving_data, inschrijving_data, session))

        concurrent.futures.wait(futures)
