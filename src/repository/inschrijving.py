from .base import Base

import logging
from sqlalchemy.orm import Mapped, mapped_column, sessionmaker, relationship
from sqlalchemy import String, Float, ForeignKey, text
from sqlalchemy.dialects.mssql import DATETIME2
from repository.main import get_engine, DATA_PATH
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
    ContactficheId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("Contactfiche.ContactpersoonId", use_alter=True), nullable=True
    )
    Contactfiche: Mapped["Contactfiche"] = relationship(back_populates="Inschrijving")
    CampagneId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("Campagne.CampagneId", use_alter=True), nullable=True
    )
    Campagne: Mapped["Campagne"] = relationship(back_populates="Inschrijving")

    SessieInschrijving: Mapped["SessieInschrijving"] = relationship(
        back_populates="Inschrijving"
    )


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

        if len(inschrijving_data) >= BATCH_SIZE:
            insert_inschrijving_data(inschrijving_data, session)
            inschrijving_data = []
            progress_bar.update(BATCH_SIZE)

    # Insert any remaining data
    if inschrijving_data:
        insert_inschrijving_data(inschrijving_data, session)
        progress_bar.update(len(inschrijving_data))

    session.execute(
        text(
            """
        UPDATE Inschrijving
        SET Inschrijving.ContactficheId = NULL
        WHERE Inschrijving.ContactficheId
        NOT IN 
        (SELECT ContactPersoonId FROM Contactfiche)
    """
        )
    )  # delete, want niet bruikbaar met null
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
