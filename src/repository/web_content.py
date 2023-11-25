from .base import Base
from .functionalities import load_csv, move_csv_file

import logging
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy import String, DateTime
from sqlalchemy.orm import sessionmaker
from repository.main import get_engine, DATA_PATH
import os
import pandas as pd
import numpy as np
from tqdm import tqdm

BATCH_SIZE = 1_000
DATE_FORMAT = "%d-%m-%Y %H:%M:%S"

logger = logging.getLogger(__name__)


class WebContent(Base):
    __tablename__ = "WebContent"
    __table_args__ = {"extend_existing": True}
    WebContentId                :Mapped[str] = mapped_column(String(50), primary_key=True)
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

def get_existing_ids(session):
    return [result[0] for result in session.query(WebContent.WebContentId).all()]


def seed_web_content():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    existing_ids = get_existing_ids(session)

    old_csv_dir = os.path.join(DATA_PATH, "old")
    new_csv_dir = os.path.join(DATA_PATH, "new")
    if not os.path.exists(old_csv_dir) or not os.path.exists(new_csv_dir):
        raise FileNotFoundError("The folders 'old' and 'new' must exist in the data folder")
    
    folder_new = new_csv_dir

    web_content_data = []
    for filename in os.listdir(folder_new): #check alle filenames in 'new'
        if filename == 'CDI web content.csv':
            csv_path = os.path.join(folder_new, filename)
    
            logger.info(f"Reading CSV: {csv_path}")
            df, error = load_csv(csv_path)
            if error:
                raise Exception(f"Error loading CSV: {csv_path, error}")
            
            df = df.replace({np.nan: None})

            df = df[~df['crm_CDI_WebContent_Campaign'].isin(existing_ids)]

            df['crm_CDI_WebContent_Created_On'] = pd.to_datetime(df['crm_CDI_WebContent_Created_On'], format=DATE_FORMAT)
            df['crm_CDI_WebContent_Modified_On'] = pd.to_datetime(df['crm_CDI_WebContent_Modified_On'], format=DATE_FORMAT)  

            # data in chunks steken
            chunks = [df[i:i + BATCH_SIZE] for i in range(0, df.shape[0], BATCH_SIZE)]

            progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)

            for chunk in chunks:
                web_content_data = []
                for _, row in chunk.iterrows():
                    p = WebContent(
                        Campaign                    = row['crm_CDI_WebContent_Campaign'],
                        CampaignName                = row['crm_CDI_WebContent_Campaign_Name'],
                        Name                        = row['crm_CDI_WebContent_Name'],
                        WebContentId                = row['crm_CDI_WebContent_Web_Content'],
                        GemaaktDoorNaam             = row['crm_CDI_WebContent_Gemaakt_door_Naam_'],
                        CreatedOn                   = row['crm_CDI_WebContent_Created_On'],
                        GewijzigdDoorNaam           = row['crm_CDI_WebContent_Gewijzigd_door_Naam_'],
                        ModifiedOn                  = row['crm_CDI_WebContent_Modified_On'],
                        Owner                       = row['crm_CDI_WebContent_Owner'],
                        OwnerName                   = row['crm_CDI_WebContent_Owner_Name'],
                        HetBezittenVanBusinessUnit  = row['crm_CDI_WebContent_Het_bezitten_van_Business_Unit'],
                    )
                    web_content_data.append(p)

                insert_web_content_data(web_content_data, session)
                progress_bar.update(len(web_content_data))
            
            progress_bar.close()

            move_csv_file(csv_path, old_csv_dir)

            logger.info(f"Number of new (non-duplicate) rows found in {csv_path}: {len(df)}")

    if not web_content_data:
        logger.info("No new data was given. Data is up to date already.")
    
    
    