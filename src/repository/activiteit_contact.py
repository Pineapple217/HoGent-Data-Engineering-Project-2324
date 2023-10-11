from .base import Base

import logging
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy import String, DateTime, Integer, Date, UniqueConstraint
from sqlalchemy.orm import sessionmaker
from repository.main import get_engine, DATA_PATH
import pandas as pd
import numpy as np
from tqdm import tqdm


BATCH_SIZE = 10_000

logger = logging.getLogger(__name__)


class ActiviteitContact(Base):
    __tablename__ = "ActiviteitContact"
    __table_args__ = {'extend_existing': True}
    ActiviteitID: Mapped[str] = mapped_column(String(255), nullable=False, primary_key=True)
    VereistContactID: Mapped[str] = mapped_column(String(255), nullable=False, primary_key=True)


def insert_ActiviteitContact_data(ActiviteitContact_data, session):
    session.bulk_save_objects(ActiviteitContact_data)
    session.commit()


def seed_activiteit_contact():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    logger.info("Reading CSV...")
    csv = DATA_PATH + '/Activiteit_vereist_contact.csv'
    df = pd.read_csv(csv, delimiter=",", encoding='utf-8-sig', keep_default_na=True, na_values=[''])
    df = df.drop_duplicates()
    df = df.replace({np.nan: None})
    
    ActiviteitContact_data = []
    logger.info("Seeding inserting rows")
    progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)
    for _, row in df.iterrows():
        ac = ActiviteitContact(
            ActiviteitID=row["crm_ActiviteitVereistContact_ActivityId"],
            VereistContactID=row["crm_ActiviteitVereistContact_ReqAttendee"],
        )

        ActiviteitContact_data.append(ac)
        
        if len(ActiviteitContact_data) >= BATCH_SIZE:
            insert_ActiviteitContact_data(ActiviteitContact_data, session)
            ActiviteitContact_data = []
            progress_bar.update(BATCH_SIZE)

    if ActiviteitContact_data:
        insert_ActiviteitContact_data(ActiviteitContact_data, session)
        progress_bar.update(len(ActiviteitContact_data))

        
        

