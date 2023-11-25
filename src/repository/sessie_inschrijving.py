from .base import Base
from .functionalities import load_csv, move_csv_file

import logging
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column, relationship
from sqlalchemy import String, ForeignKey, text
from sqlalchemy.orm import sessionmaker
from repository.main import get_engine, DATA_PATH
import os
import numpy as np
from tqdm import tqdm
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from .sessie import Sessie
    from .inschrijving import Inschrijving

BATCH_SIZE = 25_000

logger = logging.getLogger(__name__)


class SessieInschrijving(Base):
    __tablename__ = "SessieInschrijving" 
    __table_args__ = {"extend_existing": True}
    SessieInschrijvingId: Mapped[str] = mapped_column(String(50), nullable=True, primary_key=True)
    Sessie: Mapped[str] = mapped_column(String(50), nullable=True)
    Inschrijving: Mapped[str] = mapped_column(String(50), nullable=True)

    # FK
    SessieId: Mapped[Optional[str]] = mapped_column(ForeignKey("Sessie.SessieId", use_alter=True), nullable=True)
    Sessie: Mapped["Sessie"] = relationship(back_populates="SessieInschrijving")
    
    InschrijvingId: Mapped[Optional[str]] = mapped_column(ForeignKey("Inschrijving.InschrijvingId", use_alter=True), nullable=True)
    Inschrijving: Mapped["Inschrijving"] = relationship(back_populates="SessieInschrijving")


def insert_sessie_inschrijving_data(sessie_inschrijving_data, session):
    session.bulk_save_objects(sessie_inschrijving_data)
    session.commit()


'''
USAGE:
Create the directories "old" and "new" in the data folder.
Place new CSV file in the "new" folder.
Run the seeding.
The processed CSV file will be moved to the "old" folder with a timestamp.
The "new" folder will now be empty.
Place a new CSV file in the "new" folder to add more new data.
Run the seeding again.
'''

    
# geeft een lijst van alle ids die al in de database zitten door de tabel te queryen, enkel de id kolom wordt teruggegeven
def get_existing_ids(session):
    return [result[0] for result in session.query(SessieInschrijving.SessieInschrijvingId).all()]


def seed_sessie_inschrijving():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    
    # haal alle ids op die al in de database zitten
    existing_ids = get_existing_ids(session)
    
    # pad naar de csv bestanden, deze moeten in de mappen "old" en "new" zitten
    old_csv_dir = os.path.join(DATA_PATH, "old")
    new_csv_dir = os.path.join(DATA_PATH, "new")
    
    # check of de mappen "old" en "new" bestaan
    if not os.path.exists(old_csv_dir) or not os.path.exists(new_csv_dir):
        raise FileNotFoundError("The folders 'old' and 'new' must exist in the data folder")

    folder_new = new_csv_dir
    sessie_inschrijving_data = []
    
    for filename in os.listdir(folder_new):
        if filename == 'Sessie inschrijving.csv':
            csv_path = os.path.join(folder_new, filename)
            logger.info(f"Reading CSV: {csv_path}")
            df, error = load_csv(csv_path)
            if error:
                raise Exception(f"Error loading CSV: {csv_path, error}")
            
            # verwijder de rijen waarvan de id al in de database zit
            df = df[~df["crm_SessieInschrijving_SessieInschrijving"].isin(existing_ids)]
            
            df = df.dropna(how="all", axis=0)
    
            df = df.replace({np.nan: None})
    
            logger.info("Seeding inserting rows")
            progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)
            
            # data in chunks steken
            chunks = [df[i:i + BATCH_SIZE] for i in range(0, df.shape[0], BATCH_SIZE)]
            for chunk in chunks:
                sessie_inschrijving_data = []
                for _, row in chunk.iterrows(): 
                    p = SessieInschrijving(
                        SessieInschrijvingId=row["crm_SessieInschrijving_SessieInschrijving"],
                        SessieId=row["crm_SessieInschrijving_Sessie"],
                        InschrijvingId=row["crm_SessieInschrijving_Inschrijving"],
                    )
                    sessie_inschrijving_data.append(p)

                insert_sessie_inschrijving_data(sessie_inschrijving_data, session)
                progress_bar.update(len(sessie_inschrijving_data))

            progress_bar.close()

            # verplaats het csv bestand naar de "old" map voor reeds verwerkte bestanden
            move_csv_file(csv_path, old_csv_dir)

            logger.info(f"Number of new (non-duplicate) rows found in {csv_path}: {len(df)}")
    
    # als er geen nieuwe data is, dan is de lijst leeg
    if not sessie_inschrijving_data:
        logger.info("No new data was given. Data is up to date already.")
    
    session.execute(text("""
        UPDATE SessieInschrijving
        SET SessieInschrijving.SessieId = NULL
        WHERE SessieInschrijving.SessieId
        NOT IN
        (SELECT SessieId FROM Sessie)
    """))
    session.commit()

    session.execute(text("""
        UPDATE SessieInschrijving
        SET SessieInschrijving.InschrijvingId = NULL
        WHERE SessieInschrijving.InschrijvingId
        NOT IN
        (SELECT InschrijvingId FROM Inschrijving)
    """))
    session.commit()
        
    session.close()