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


BATCH_SIZE = 1_000

logger = logging.getLogger(__name__)


class WebContent(Base):
    __tablename__ = "WebContent"
    __table_args__ = {"extend_existing": True}
    WebContentId                  :Mapped[str] = mapped_column(String(50), primary_key=True)
    Campaign                    :Mapped[str] = mapped_column(String(50), nullable=True)
    CampaignName                :Mapped[str] = mapped_column(String(200), nullable=True)
    Name                        :Mapped[str] = mapped_column(String(200))
    GemaaktDoorNaam             :Mapped[str] = mapped_column(String(50))
    CreatedOn                   :Mapped[DateTime] = mapped_column(DateTime)
    GewijzigdDoorNaam           :Mapped[str] = mapped_column(String(50))
    ModifiedOn                  :Mapped[DateTime] = mapped_column(DateTime)
    Owner                       :Mapped[str] = mapped_column(String(50))
    OwnerName                   :Mapped[str] = mapped_column(String(50))
    HetBezittenVanBusinessUnit  :Mapped[str] = mapped_column(String(50))


def insert_web_content_data(pageviews_data, session):
    session.bulk_save_objects(pageviews_data)
    session.commit()


def seed_web_content():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    logger.info("Reading CSV...")
    csv = DATA_PATH + "/CDI web content.csv"
    df = pd.read_csv(
        csv, delimiter=",", encoding="utf-8", keep_default_na=True, na_values=[""]
    )
    df = df.replace({np.nan: None})
    # Sommige lege waardes worden als NaN ingelezeno
    # NaN mag niet in een varchar
    df['crm_CDI_WebContent_Created_On'] = pd.to_datetime(
        df['crm_CDI_WebContent_Created_On'], format="%d-%m-%Y %H:%M:%S"
    )
    df['crm_CDI_WebContent_Modified_On'] = pd.to_datetime(
        df['crm_CDI_WebContent_Modified_On'], format="%d-%m-%Y %H:%M:%S"
    )
    send_web_content_data = []
    logger.info("Seeding inserting rows")
    progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)
    for _, row in df.iterrows():

        p = WebContent(
            Campaign                    = row['crm_CDI_WebContent_Campaign'],
            CampaignName                = row['crm_CDI_WebContent_Campaign_Name'],
            Name                        = row['crm_CDI_WebContent_Name'],
            WebContentId                  = row['crm_CDI_WebContent_Web_Content'],
            GemaaktDoorNaam             = row['crm_CDI_WebContent_Gemaakt_door_Naam_'],
            CreatedOn                   = row['crm_CDI_WebContent_Created_On'],
            GewijzigdDoorNaam           = row['crm_CDI_WebContent_Gewijzigd_door_Naam_'],
            ModifiedOn                  = row['crm_CDI_WebContent_Modified_On'],
            Owner                       = row['crm_CDI_WebContent_Owner'],
            OwnerName                   = row['crm_CDI_WebContent_Owner_Name'],
            HetBezittenVanBusinessUnit  = row['crm_CDI_WebContent_Het_bezitten_van_Business_Unit'],
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
