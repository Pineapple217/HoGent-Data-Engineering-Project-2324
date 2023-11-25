from .base import Base
from .functionalities import load_csv, move_csv_file

import logging
from sqlalchemy.orm import Mapped, mapped_column, sessionmaker, relationship
from sqlalchemy import String
from repository.main import get_engine, DATA_PATH
import os
import numpy as np
from tqdm import tqdm

BATCH_SIZE = 10

logger = logging.getLogger(__name__)


class Functie(Base):
    __tablename__ = "Functie"
    __table_args__ = {"extend_existing": True}
    FunctieId: Mapped[str] = mapped_column(String(50), primary_key=True)
    Naam: Mapped[str] = mapped_column(String(75))
    
    # FK
    ContactficheFunctie: Mapped["ContactficheFunctie"] = relationship(back_populates="Functie")


def insert_functie_data(functie_data, session):
    session.bulk_save_objects(functie_data)
    session.commit()

def get_existing_ids(session):
    return [result[0] for result in session.query(Functie.FunctieId).all()]


def seed_functie():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    existing_ids = get_existing_ids(session)

    old_csv_dir = os.path.join(DATA_PATH, "old")
    new_csv_dir = os.path.join(DATA_PATH, "new")
    if not os.path.exists(old_csv_dir) or not os.path.exists(new_csv_dir):
        raise FileNotFoundError("The folders 'old' and 'new' must exist in the data folder")
    
    folder_new = new_csv_dir

    functie_data = []
    for filename in os.listdir(folder_new): #check alle filenames in 'new'
        if filename == 'Functie.csv':
            csv_path = os.path.join(folder_new, filename)
    
            logger.info(f"Reading CSV: {csv_path}")
            df, error = load_csv(csv_path)
            if error:
                raise Exception(f"Error loading CSV: {csv_path, error}")
            
            df = df.replace({np.nan: None})

            df = df[~df['crm_Functie_Functie'].isin(existing_ids)]  

            # data in chunks steken
            chunks = [df[i:i + BATCH_SIZE] for i in range(0, df.shape[0], BATCH_SIZE)]

            progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)

            for chunk in chunks:
                functie_data = []
                for _, row in chunk.iterrows():
                    p = Functie(
                        FunctieId=row["crm_Functie_Functie"],
                        Naam=row["crm_Functie_Naam"],
                    )
                    functie_data.append(p)

                insert_functie_data(functie_data, session)
                progress_bar.update(len(functie_data))

            progress_bar.close()

            move_csv_file(csv_path, old_csv_dir)

            logger.info(f"Number of new (non-duplicate) rows found in {csv_path}: {len(df)}")

    if not functie_data:
        logger.info("No new data was given. Data is up to date already.")