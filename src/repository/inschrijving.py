from .base import Base

import logging
from sqlalchemy.orm import Mapped, mapped_column, sessionmaker
from sqlalchemy import String, Float
from sqlalchemy.dialects.mssql import DATETIME2
from repository.main import get_engine, DATA_PATH
import pandas as pd
import numpy as np
from tqdm import tqdm

BATCH_SIZE = 10_000

logger = logging.getLogger(__name__)

class Inschrijving(Base):
    __tablename__ = "Inschrijving"
    __table_args__ = {"extend_existing": True}
    Inschrijving: Mapped[str] = mapped_column(String(50), primary_key=True)
    AanwezigAfwezig: Mapped[str] = mapped_column(String(50))
    Bron: Mapped[str] = mapped_column(String(20), nullable=True)
    Contactfiche: Mapped[str] = mapped_column(String(50), nullable=True)
    DatumInschrijving: Mapped[DATETIME2] = mapped_column(DATETIME2)
    FacturatieBedrag: Mapped[Float] = mapped_column(Float)
    Campagne: Mapped[str] = mapped_column(String(50))
    CampagneNaam: Mapped[str] = mapped_column(String(200))


def insert_inschrijving_data(inschrijving_data, session):
    session.bulk_save_objects(inschrijving_data)
    session.commit()


def to_float(x):
    try:
        return float(x)
    except:
        return float(x.replace(",", ".").replace("'", ""))


def seed_inschrijving():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    logger.info("Reading CSV...")
    csv = DATA_PATH + "/Inschrijvingen.csv"
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
    for i, row in df.iterrows():
        p = Inschrijving(
            Inschrijving=row["crm_Inschrijving_Inschrijving"],
            AanwezigAfwezig=row["crm_Inschrijving_Aanwezig_Afwezig"],
            Bron=row["crm_Inschrijving_Bron"],
            Contactfiche=row["crm_Inschrijving_Contactfiche"],
            DatumInschrijving=row["crm_Inschrijving_Datum_inschrijving"],
            FacturatieBedrag=to_float(row["crm_Inschrijving_Facturatie_Bedrag"]),
            Campagne=row["crm_Inschrijving_Campagne"],
            CampagneNaam=row["crm_Inschrijving_Campagne_Naam_"],
        )

        inschrijving_data.append(p)

        if len(inschrijving_data) >= BATCH_SIZE:
            insert_inschrijving_data(inschrijving_data, session)
            inschrijving_data = []
            progress_bar.update(BATCH_SIZE)

    # Insert any remaining data
    if inschrijving_data:
        insert_inschrijving_data(inschrijving_data, session)
        progress_bar.update(len(inschrijving_data))
