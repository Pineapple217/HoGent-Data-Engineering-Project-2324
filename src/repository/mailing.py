from .base import Base

import logging
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column, relationship
from sqlalchemy import String, Date
from sqlalchemy.orm import sessionmaker
from repository.main import get_engine, DATA_PATH
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
    Mailing     :Mapped[str]  = mapped_column(String(50), primary_key=True)
    Name        :Mapped[str]  = mapped_column(String(200), nullable=True)
    SentOn      :Mapped[Date] = mapped_column(Date)
    Onderwerp   :Mapped[str]  = mapped_column(String(200))
    SendClicks  :Mapped["SendEmailClicks"] = relationship(back_populates="EmailVersturen")
    Visits      :Mapped["Visit"] = relationship(back_populates="EmailSend")


def insert_mailings_data(pageviews_data, session):
    session.bulk_save_objects(pageviews_data)
    session.commit()


def seed_mailing():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    logger.info("Reading CSV...")
    csv = DATA_PATH + "/CDI mailing.csv"
    df = pd.read_csv(
        csv, delimiter=",", encoding="utf-8", keep_default_na=True, na_values=[""]
    )
    df = df.replace({np.nan: None})
    # Sommige lege waardes worden als NaN ingelezeno
    # NaN mag niet in een varchar
    df["crm_CDI_Mailing_Sent_On"] = pd.to_datetime(
        df["crm_CDI_Mailing_Sent_On"], format="%d-%m-%Y"
    )
    mailing_data = []
    logger.info("Seeding inserting rows")
    progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)
    for _, row in df.iterrows():
        p = Mailing(
            Mailing     = row['crm_CDI_Mailing_Mailing'],
            Name        = row['crm_CDI_Mailing_Name'],
            SentOn      = row['crm_CDI_Mailing_Sent_On'],
            Onderwerp   = row['crm_CDI_Mailing_Onderwerp'],
        )
        mailing_data.append(p)

        if len(mailing_data) >= BATCH_SIZE:
            insert_mailings_data(mailing_data, session)
            mailing_data = []
            progress_bar.update(BATCH_SIZE)

    # Insert any remaining data
    if mailing_data:
        insert_mailings_data(mailing_data, session)
        progress_bar.update(len(mailing_data))
