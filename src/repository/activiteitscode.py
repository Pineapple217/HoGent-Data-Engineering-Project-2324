from .base import Base
from .functionalities import load_csv, move_csv_file

import logging
from sqlalchemy.orm import Mapped, mapped_column, sessionmaker, relationship
from sqlalchemy import String, Boolean
from repository.main import get_engine, DATA_PATH
import os
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

    # FK
    AccountActiviteitscode   :Mapped["AccountActiviteitscode"] = relationship(back_populates="Activiteit")


def insert_activiteitscode_data(activiteitscode_data, session):
    session.bulk_save_objects(activiteitscode_data)
    session.commit()

def get_existing_ids(session):
    return [result[0] for result in session.query(Activiteitscode.ActiviteitsId).all()]


def seed_activiteitscode():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    existing_ids = get_existing_ids(session)

    old_csv_dir = os.path.join(DATA_PATH, "old")
    new_csv_dir = os.path.join(DATA_PATH, "new")
    if not os.path.exists(old_csv_dir) or not os.path.exists(new_csv_dir):
        raise FileNotFoundError("The folders 'old' and 'new' must exist in the data folder")
    
    folder_new = new_csv_dir

    activiteitscode_data = []
    for filename in os.listdir(folder_new): #check alle filenames in 'new'
        if filename == 'Activiteitscode.csv':
            csv_path = os.path.join(folder_new, filename)
    
            logger.info(f"Reading CSV: {csv_path}")
            df, error = load_csv(csv_path)
            if error:
                raise Exception(f"Error loading CSV: {csv_path, error}")
            
            df = df.replace({np.nan: None})
            df['crm_ActiviteitsCode_Status'] = df['crm_ActiviteitsCode_Status'].replace({'Actief': True, 'Inactief': False})

            df = df[~df['crm_ActiviteitsCode_Activiteitscode'].isin(existing_ids)]  

            # data in chunks steken
            chunks = [df[i:i + BATCH_SIZE] for i in range(0, df.shape[0], BATCH_SIZE)]

            progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)

            for chunk in chunks:
                activiteitscode_data = []
                for _, row in chunk.iterrows():
                    p = Activiteitscode(
                        ActiviteitsId=row["crm_ActiviteitsCode_Activiteitscode"],
                        Naam=row["crm_ActiviteitsCode_Naam"],
                        Status=row["crm_ActiviteitsCode_Status"],
                    )
                    activiteitscode_data.append(p)

                insert_activiteitscode_data(activiteitscode_data, session)
                progress_bar.update(len(activiteitscode_data))

            progress_bar.close()

            move_csv_file(csv_path, old_csv_dir)

            logger.info(f"Number of new (non-duplicate) rows found in {csv_path}: {len(df)}")

    if not activiteitscode_data:
        logger.info("No new data was given. Data is up to date already.")