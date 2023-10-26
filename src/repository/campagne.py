from .base import Base

import logging
from sqlalchemy.orm import Mapped, mapped_column, sessionmaker, relationship
from sqlalchemy import String
from sqlalchemy.dialects.mssql import DATETIME2
from repository.main import get_engine, DATA_PATH
import pandas as pd
import numpy as np
from typing import TYPE_CHECKING
from tqdm import tqdm

if TYPE_CHECKING:
    from .pageviews import Pageview
    from .sessie import Sessie

BATCH_SIZE = 10_000

logger = logging.getLogger(__name__)


class Campagne(Base):
    __tablename__ = "Campagne"  # snakecase
    __table_args__ = {"extend_existing": True}
    Campagne: Mapped[str] = mapped_column(String(50), primary_key=True)
    CampagneNr: Mapped[str] = mapped_column(String(50), nullable=True)
    Einddatum: Mapped[DATETIME2] = mapped_column(DATETIME2)
    CampagneNaam: Mapped[str] = mapped_column(String(200))
    NaamInMail: Mapped[str] = mapped_column(String(200))
    RedenVanStatus: Mapped[str] = mapped_column(String(50))
    Startdatum: Mapped[DATETIME2] = mapped_column(DATETIME2)
    Status: Mapped[str] = mapped_column(String(50))
    TypeCampagne: Mapped[str] = mapped_column(String(50))
    URLVoka: Mapped[str] = mapped_column(String(150), nullable=True)
    SoortCampagne: Mapped[str] = mapped_column(String(50))

    Pageviews: Mapped["Pageview"] = relationship(back_populates="Campagne")

    Sessie: Mapped["Sessie"] = relationship(back_populates="Campagne")


def insert_campagne_data(campagne_data, session):
    session.bulk_save_objects(campagne_data)
    session.commit()


def seed_campagne():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    logger.info("Reading CSV...")
    csv = DATA_PATH + "/Campagnes.csv"
    df = pd.read_csv(
        csv,
        delimiter=",",
        encoding="utf-8",
        keep_default_na=True,
        na_values=[""],
    )
    df = df.replace({np.nan: None})

    df["crm_Campagne_Einddatum"] = pd.to_datetime(
        df["crm_Campagne_Einddatum"], format="%d-%m-%Y %H:%M:%S"
    )
    df["crm_Campagne_Startdatum"] = pd.to_datetime(
        df["crm_Campagne_Startdatum"], format="%d-%m-%Y %H:%M:%S"
    )

    campagne_data = []
    logger.info("Seeding inserting rows")
    progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)
    for _, row in df.iterrows():
        p = Campagne(
            Campagne=row["crm_Campagne_Campagne"],
            CampagneNr=row["crm_Campagne_Campagne_Nr"],
            Einddatum=row["crm_Campagne_Einddatum"],
            CampagneNaam=row["crm_Campagne_Naam"],
            NaamInMail=row["crm_Campagne_Naam_in_email"],
            RedenVanStatus=row["crm_Campagne_Reden_van_status"],
            Startdatum=row["crm_Campagne_Startdatum"],
            Status=row["crm_Campagne_Status"],
            TypeCampagne=row["crm_Campagne_Type_campagne"],
            URLVoka=row["crm_Campagne_URL_voka_be"],
            SoortCampagne=row["crm_Campagne_Soort_Campagne"],
        )
        campagne_data.append(p)

        if len(campagne_data) >= BATCH_SIZE:
            insert_campagne_data(campagne_data, session)
            campagne_data = []
            progress_bar.update(BATCH_SIZE)

    if campagne_data:
        insert_campagne_data(campagne_data, session)
        progress_bar.update(len(campagne_data))
