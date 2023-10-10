from .base import Base

import logging
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy import String, DateTime, Integer, Date
from sqlalchemy.orm import sessionmaker
from repository.main import get_engine, DATA_PATH
import pandas as pd
import numpy as np
from tqdm import tqdm


BATCH_SIZE = 10_000

logger = logging.getLogger(__name__)


class WebContent(Base):
    __tablename__ = "WebContent"
    __table_args__ = {"extend_existing": True}
    Campaign
    CampaignName
    Name
    WebContent
    GemaaktSoorNaam
    CreatedOn
    GewijzigdDoorNaam
    ModifiedOn
    Owner
    OwnerName
    HetBezittenVanBusinessUnit

    SentEmail       :Mapped[str] = mapped_column(String(50), primary_key=True)
    Clicks          :Mapped[int] = mapped_column(Integer)
    Contact         :Mapped[str] = mapped_column(String(50))
    EmailVersturen  :Mapped[str] = mapped_column(String(50))


def insert_web_content_data(pageviews_data, session):
    session.bulk_save_objects(pageviews_data)
    session.commit()


def seed_send_email_clicks():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    logger.info("Reading CSV...")
    csv = DATA_PATH + "/CDI sent email clicks.csv"
    df = pd.read_csv(
        csv, delimiter=",", encoding="utf-8", keep_default_na=True, na_values=[""]
    )
    df = df.replace({np.nan: None})
    # Sommige lege waardes worden als NaN ingelezeno
    # NaN mag niet in een varchar
    send_web_content_data = []
    logger.info("Seeding inserting rows")
    progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)
    for _, row in df.iterrows():
        p = WebContent(
            SentEmail       = row['crm_CDI_SentEmail_kliks_Sent_Email'],
            Clicks          = row['crm_CDI_SentEmail_kliks_Clicks'],
            Contact         = row['crm_CDI_SentEmail_kliks_Contact'],
            EmailVersturen  = row['crm_CDI_SentEmail_kliks_E_mail_versturen'],
        )
        send_web_content_data.append(p)

        if len(send_web_content_data) >= BATCH_SIZE:
            insert_web_content_data(send_web_content_data, session)
            send_web_content_data = []
            progress_bar.update(BATCH_SIZE)

    # Insert any remaining data
    if send_web_content_data:
        insert_web_content_data(send_web_content_data, session)
        progress_bar.update(len(send_web_content_data))
