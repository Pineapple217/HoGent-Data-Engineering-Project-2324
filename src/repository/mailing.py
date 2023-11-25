from .base import Base
from .functionalities import load_csv, move_csv_file

import logging
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column, relationship
from sqlalchemy import String, Date
from sqlalchemy.orm import sessionmaker
from repository.main import get_engine, DATA_PATH
import os
import pandas as pd
import numpy as np
from tqdm import tqdm
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .send_email_clicks import SendEmailClicks
    from .visits import Visit

BATCH_SIZE = 10_000

logger = logging.getLogger(__name__)


class Mailing(Base):
    __tablename__ = "Mailing"
    __table_args__ = {"extend_existing": True}
    MailingId   :Mapped[str]  = mapped_column(String(50), primary_key=True)
    Name        :Mapped[str]  = mapped_column(String(200), nullable=True)
    SentOn      :Mapped[Date] = mapped_column(Date)
    Onderwerp   :Mapped[str]  = mapped_column(String(200))

    # FK
    SendClicks  :Mapped["SendEmailClicks"] = relationship(back_populates="EmailVersturen")
    Visits      :Mapped["Visit"] = relationship(back_populates="EmailSend")


def insert_mailings_data(pageviews_data, session):
    session.bulk_save_objects(pageviews_data)
    session.commit()

def get_existing_ids(session):
    return [result[0] for result in session.query(Mailing.MailingId).all()]

def seed_mailing():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    existing_ids = get_existing_ids(session)

    old_csv_dir = os.path.join(DATA_PATH, "old")
    new_csv_dir = os.path.join(DATA_PATH, "new")
    if not os.path.exists(old_csv_dir) or not os.path.exists(new_csv_dir):
        raise FileNotFoundError("The folders 'old' and 'new' must exist in the data folder")
    
    folder_new = new_csv_dir

    mailing_data = []
    for filename in os.listdir(folder_new): #check alle filenames in 'new'
        if filename == 'CDI mailing.csv': #hardcoded filename, zodat startswith niet fout kan lopen
            csv_path = os.path.join(folder_new, filename) #vul filepath aan met gevonden file
    
            logger.info(f"Reading CSV: {csv_path}")
            df, error = load_csv(csv_path) #load_csv uit functionalities.py, probeert met hardcoded delimiters en encodings een df te maken
            if error:
                raise Exception(f"Error loading CSV: {csv_path, error}")
            
            df = df.replace({np.nan: None})
            df["crm_CDI_Mailing_Sent_On"] = pd.to_datetime(df["crm_CDI_Mailing_Sent_On"], format="%d-%m-%Y")

            df = df[~df['crm_CDI_Mailing_Mailing'].isin(existing_ids)]

            chunks = [df[i:i + BATCH_SIZE] for i in range(0, df.shape[0], BATCH_SIZE)]

            progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)

            for chunk in chunks:
                mailing_data = []
                for _, row in chunk.iterrows():
                    p = Mailing(
                        MailingId   = row['crm_CDI_Mailing_Mailing'],
                        Name        = row['crm_CDI_Mailing_Name'],
                        SentOn      = row['crm_CDI_Mailing_Sent_On'],
                        Onderwerp   = row['crm_CDI_Mailing_Onderwerp'],
                    )
                    mailing_data.append(p)

                insert_mailings_data(mailing_data, session)
                progress_bar.update(len(mailing_data))

            progress_bar.close()

            move_csv_file(csv_path, old_csv_dir)

            logger.info(f"Number of new (non-duplicate) rows found in {csv_path}: {len(df)}")

    if not mailing_data:
        logger.info("No new data was given. Data is up to date already.")