from .base import Base

import logging
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column, relationship
from sqlalchemy import String, ForeignKey, text
from sqlalchemy.orm import sessionmaker
from repository.main import get_engine, DATA_PATH
import pandas as pd
import numpy as np
from tqdm import tqdm
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from .sessie import Sessie
    from .inschrijving import Inschrijving

BATCH_SIZE = 10_000

logger = logging.getLogger(__name__)


class SessieInschrijving(Base):
    __tablename__ = "SessieInschrijving" 
    __table_args__ = {"extend_existing": True}
    SessieInschrijvingId: Mapped[str] = mapped_column(String(50), nullable=True, primary_key=True)
    Sessie: Mapped[str] = mapped_column(String(50), nullable=True)
    Inschrijving: Mapped[str] = mapped_column(String(50), nullable=True)

    # FK
    SessieId: Mapped[Optional[str]] = mapped_column(ForeignKey("Sessie.SessieId", use_alter=True), nullable=True)
    Sessie: Mapped["Sessie"] = relationship(back_populates="SessieInschrijving")
    
    InschrijvingId: Mapped[Optional[str]] = mapped_column(ForeignKey("Inschrijving.InschrijvingId", use_alter=True), nullable=True)
    Inschrijving: Mapped["Inschrijving"] = relationship(back_populates="SessieInschrijving")


def insert_sessie_inschrijving_data(sessie_inschrijving_data, session):
    session.bulk_save_objects(sessie_inschrijving_data)
    session.commit()


def seed_sessie_inschrijving():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    logger.info("Reading CSV...")
    csv = DATA_PATH + "/Sessie inschrijving.csv"
    df = pd.read_csv(csv, delimiter=",", encoding="utf-8", keep_default_na=True, na_values=[""],)
    
    # csv bevat veel lege rijen, dit drop alle rijen die volledig leeg zijn
    df = df.dropna(how="all", axis=0)
    
    # Sommige lege waardes worden als NaN ingelezen
    # NaN mag niet in een varchar
    df = df.replace({np.nan: None})
    
    sessie_inschrijving_data = []
    logger.info("Seeding inserting rows")
    progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)
    for _, row in df.iterrows():
        p = SessieInschrijving(
            SessieInschrijvingId=row["crm_SessieInschrijving_SessieInschrijving"],
            SessieId=row["crm_SessieInschrijving_Sessie"],
            InschrijvingId=row["crm_SessieInschrijving_Inschrijving"],
        )
        sessie_inschrijving_data.append(p)

        if len(sessie_inschrijving_data) >= BATCH_SIZE:
            insert_sessie_inschrijving_data(sessie_inschrijving_data, session)
            sessie_inschrijving_data = []
            progress_bar.update(BATCH_SIZE)

    # Insert any remaining data
    if sessie_inschrijving_data:
        insert_sessie_inschrijving_data(sessie_inschrijving_data, session)
        progress_bar.update(len(sessie_inschrijving_data))

    session.execute(
        text(
            """
        UPDATE SessieInschrijving
        SET SessieInschrijving.SessieId = NULL
        WHERE SessieInschrijving.SessieId
        NOT IN
        (SELECT SessieId FROM Sessie)
    """
        )
    )
    session.commit()
    
    session.execute(
        text(
            """
        UPDATE SessieInschrijving
        SET SessieInschrijving.InschrijvingId = NULL
        WHERE SessieInschrijving.InschrijvingId
        NOT IN
        (SELECT InschrijvingId FROM Inschrijving)
    """
        )
    )
    session.commit()
