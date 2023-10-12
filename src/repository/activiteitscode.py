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


BATCH_SIZE = 10
logger = logging.getLogger(__name__)


class Activiteitscode(Base):
    __tablename__ = "Activiteitscode"  # snakecase
    __table_args__ = {"extend_existing": True}
    Activiteitscode: Mapped[str] = mapped_column(
        String(50), nullable=False, primary_key=True
    )
    Naam: Mapped[str] = mapped_column(String(50), nullable=True)
    Status: Mapped[str] = mapped_column(String(20), nullable=True)


def insert_activiteitscode_data(activiteitscode_data, session):
    session.bulk_save_objects(activiteitscode_data)
    session.commit()


def seed_activiteitscode():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    logger.info("Reading CSV...")
    csv = DATA_PATH + "/Activiteitscode.csv"
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
    activiteitscode_data = []
    logger.info("Seeding inserting rows")
    progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)
    for i, row in df.iterrows():
        p = Activiteitscode(
            Activiteitscode=row["crm_ActiviteitsCode_Activiteitscode"],
            Naam=row["crm_ActiviteitsCode_Naam"],
            Status=row["crm_ActiviteitsCode_Status"],
        )

        activiteitscode_data.append(p)

        if len(activiteitscode_data) >= BATCH_SIZE:
            insert_activiteitscode_data(activiteitscode_data, session)
            activiteitscode_data = []
            progress_bar.update(BATCH_SIZE)

    # Insert any remaining data
    if activiteitscode_data:
        insert_activiteitscode_data(activiteitscode_data, session)
        progress_bar.update(len(activiteitscode_data))
