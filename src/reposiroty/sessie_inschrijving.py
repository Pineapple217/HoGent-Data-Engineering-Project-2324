from .base import Base

import logging
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy import String, DateTime, Numeric, Integer, Date
from sqlalchemy.orm import sessionmaker
from repository import get_engine
import pandas as pd
import numpy as np
from tqdm import tqdm

logger = logging.getLogger(__name__)


class SessieInschrijving(Base):
    __tablename__ = "sessie_inschrijving"  # snakecase
    __table_args__ = {"extend_existing": True}
    Sessie_Inschrijving: Mapped[str] = mapped_column(
        String(50), nullable=False, primary_key=True
    )
    Sessie: Mapped[str] = mapped_column(String(50), nullable=True)
    Inschrijving: Mapped[str] = mapped_column(String(50), nullable=True)


def seed_sessie_inschrijving():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    logger.info("Reading CSV...")
    df = pd.read_csv(
        "../Data/Sessie inschrijving.csv",
        delimiter=",",
        encoding="utf-8",
        keep_default_na=True,
        na_values=[""],
    )
    df = df.replace({np.nan: None})
    # Sommige lege waardes worden als NaN ingelezeno
    # NaN mag niet in een varchar
    sessie_inschrijving_data = []
    BATCH_SIZE = 10_000
    logger.info("Seeding inserting rows")
    progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)
    for i, row in df.iterrows():
        p = SessieInschrijving(
            Sessie_Inschrijving=row["crm_SessieInschrijving_SessieInschrijving"],
            Sessie=row["crm_SessieInschrijving_Sessie"],
            Inschrijving=row["crm_SessieInschrijving_Inschrijving"],
        )
        sessie_inschrijving_data.append(p)

        if len(sessie_inschrijving_data) >= BATCH_SIZE:
            session.bulk_save_objects(sessie_inschrijving_data)
            session.commit()
            sessie_inschrijving_data = []
            progress_bar.update(BATCH_SIZE)

    # Insert any remaining data
    if sessie_inschrijving_data:
        session.bulk_save_objects(sessie_inschrijving_data)
        session.commit()
