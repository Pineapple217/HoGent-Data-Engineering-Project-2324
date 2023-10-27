from .base import Base

import logging
from sqlalchemy.orm import Mapped, mapped_column, sessionmaker, relationship
from sqlalchemy import String, Boolean
from repository.main import get_engine, DATA_PATH
import pandas as pd
import numpy as np
from tqdm import tqdm
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from account_activiteitscode import AccountActiviteitscode


BATCH_SIZE = 10

logger = logging.getLogger(__name__)

class Activiteitscode(Base):
    __tablename__ = "Activiteitscode"  
    __table_args__ = {"extend_existing": True}
    ActiviteitsId: Mapped[str] = mapped_column(String(50), primary_key=True)
    Naam: Mapped[str] = mapped_column(String(50))
    Status: Mapped[bool] = mapped_column(Boolean)

    AccountActiviteitscode   :Mapped["AccountActiviteitscode"] = relationship(back_populates="Activiteit")


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

    df['crm_ActiviteitsCode_Status'] = df['crm_ActiviteitsCode_Status'].replace({'Actief': True, 'Inactief': False})
    df = df.replace({np.nan: None})
    activiteitscode_data = []
    logger.info("Seeding inserting rows")
    progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)
    
    for i, row in df.iterrows():
        p = Activiteitscode(
            ActiviteitsId=row["crm_ActiviteitsCode_Activiteitscode"],
            Naam=row["crm_ActiviteitsCode_Naam"],
            Status=row["crm_ActiviteitsCode_Status"],
        )

        activiteitscode_data.append(p)

        if len(activiteitscode_data) >= BATCH_SIZE:
            insert_activiteitscode_data(activiteitscode_data, session)
            activiteitscode_data = []
            progress_bar.update(BATCH_SIZE)

    if activiteitscode_data:
        insert_activiteitscode_data(activiteitscode_data, session)
        progress_bar.update(len(activiteitscode_data))
