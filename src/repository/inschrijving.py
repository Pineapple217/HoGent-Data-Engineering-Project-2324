from .base import Base
from .functionalities import load_csv, move_csv_file

import logging
from sqlalchemy.orm import Mapped, mapped_column, sessionmaker, relationship
from sqlalchemy import String, Float, ForeignKey, text
from sqlalchemy.dialects.mssql import DATETIME2
from repository.main import get_engine, DATA_PATH
import os
import pandas as pd
import numpy as np
from tqdm import tqdm
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from .contactfiche import Contactfiche
    from .campagne import Campagne
    from .sessie_inschrijving import SessieInschrijving

BATCH_SIZE = 10_000

logger = logging.getLogger(__name__)


class Inschrijving(Base):
    __tablename__ = "Inschrijving"
    __table_args__ = {"extend_existing": True}
    InschrijvingId: Mapped[str] = mapped_column(String(50), primary_key=True)
    AanwezigAfwezig: Mapped[str] = mapped_column(String(50))
    Bron: Mapped[str] = mapped_column(String(20), nullable=True)
    DatumInschrijving: Mapped[DATETIME2] = mapped_column(DATETIME2)
    FacturatieBedrag: Mapped[Float] = mapped_column(Float)
    CampagneNaam: Mapped[str] = mapped_column(String(200))
    
    # FK
    ContactficheId: Mapped[Optional[str]] = mapped_column(ForeignKey("Contactfiche.ContactpersoonId", use_alter=True), nullable=True)
    Contactfiche: Mapped["Contactfiche"] = relationship(back_populates="Inschrijving")
    
    CampagneId: Mapped[Optional[str]] = mapped_column(ForeignKey("Campagne.CampagneId", use_alter=True), nullable=True)
    Campagne: Mapped["Campagne"] = relationship(back_populates="Inschrijving")

    SessieInschrijving: Mapped["SessieInschrijving"] = relationship(back_populates="Inschrijving")


def insert_inschrijving_data(inschrijving_data, session):
    session.bulk_save_objects(inschrijving_data)
    session.commit()


def to_float(x):
    try:
        return float(x)
    except:
        return float(x.replace(",", ".").replace("'", ""))

def get_existing_ids(session):
    return [result[0] for result in session.query(Inschrijving.InschrijvingId).all()]

def seed_inschrijving():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    existing_ids = get_existing_ids(session)

    old_csv_dir = os.path.join(DATA_PATH, "old")
    new_csv_dir = os.path.join(DATA_PATH, "new")
    if not os.path.exists(old_csv_dir) or not os.path.exists(new_csv_dir):
        raise FileNotFoundError("The folders 'old' and 'new' must exist in the data folder")
    
    folder_new = new_csv_dir

    inschrijving_data = []
    for filename in os.listdir(folder_new): #check alle filenames in 'new'
        #Oppassen met inschrijving: de hele eerste file heeft inschrijving als naam, maar is niet in zelfde formaat als nieuwere 2 files
        if filename == 'Inschrijving.csv' or filename == "Inschrijvingen.csv": #kan ook met startswith
            csv_path = os.path.join(folder_new, filename)
    
            logger.info(f"Reading CSV: {csv_path}")
            df, error = load_csv(csv_path)
            if error:
                raise Exception(f"Error loading CSV: {csv_path, error}")
            
            df = df.replace({np.nan: None})

            df = df[~df['crm_Inschrijving_Inschrijving'].isin(existing_ids)]  

            df["crm_Inschrijving_Datum_inschrijving"] = pd.to_datetime(df["crm_Inschrijving_Datum_inschrijving"], format="%d-%m-%Y")

            # data in chunks steken
            chunks = [df[i:i + BATCH_SIZE] for i in range(0, df.shape[0], BATCH_SIZE)]

            progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)

            for chunk in chunks:
                inschrijving_data = []
                for _, row in chunk.iterrows():
                    p = Inschrijving(
                        InschrijvingId=row["crm_Inschrijving_Inschrijving"],
                        AanwezigAfwezig=row["crm_Inschrijving_Aanwezig_Afwezig"],
                        Bron=row["crm_Inschrijving_Bron"],
                        ContactficheId=row["crm_Inschrijving_Contactfiche"],
                        DatumInschrijving=row["crm_Inschrijving_Datum_inschrijving"],
                        FacturatieBedrag=to_float(row["crm_Inschrijving_Facturatie_Bedrag"]),
                        CampagneId=row["crm_Inschrijving_Campagne"],
                        CampagneNaam=row["crm_Inschrijving_Campagne_Naam_"],
                    )
                    inschrijving_data.append(p)

                insert_inschrijving_data(inschrijving_data, session)
                progress_bar.update(len(inschrijving_data))

            progress_bar.close()

            move_csv_file(csv_path, old_csv_dir)

            logger.info(f"Number of new (non-duplicate) rows found in {csv_path}: {len(df)}")

    if not inschrijving_data:
        logger.info("No new data was given. Data is up to date already.")

    session.execute(
        text(
            """
        UPDATE Inschrijving
        SET Inschrijving.ContactficheId = NULL
        WHERE Inschrijving.ContactficheId
        NOT IN 
        (SELECT ContactpersoonId FROM Contactfiche)
    """
        )
    )
    session.commit()

    session.execute(
        text(
            """
        UPDATE Inschrijving
        SET Inschrijving.CampagneId = NULL
        WHERE Inschrijving.CampagneId
        NOT IN
        (SELECT CampagneId FROM Campagne)
    """
        )
    )
    session.commit()
