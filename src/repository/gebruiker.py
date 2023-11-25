from .base import Base
from .functionalities import load_csv, move_csv_file

import logging
from sqlalchemy.orm import Mapped, mapped_column, sessionmaker, relationship
from sqlalchemy import String
from repository.main import get_engine, DATA_PATH
import os
import numpy as np
from tqdm import tqdm

BATCH_SIZE = 10_000

logger = logging.getLogger(__name__)


class Gebruiker(Base):
    __tablename__ = "Gebruiker"
    __table_args__ = {"extend_existing": True}
    GebruikerId: Mapped[str] = mapped_column(String(50), primary_key=True)
    BusinessUnitNaam: Mapped[str] = mapped_column(String(50))
    
    # FK
    InfoEnKlachten: Mapped["InfoEnKlachten"] = relationship(back_populates="Eigenaar")
    

def insert_gebruiker_data(gebruiker_data, session):
    session.bulk_save_objects(gebruiker_data)
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
    return [result[0] for result in session.query(Gebruiker.GebruikerId).all()]


def seed_gebruiker():
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
    gebruiker_data = []
    
        # enkel de csv bestanden in de "new" map worden ingelezen, de bestanden in de "old" map zijn reeds verwerkt
    for filename in os.listdir(folder_new):
        if filename == 'Gebruikers.csv':
            csv_path = os.path.join(folder_new, filename)
            logger.info(f"Reading CSV: {csv_path}")
            df, error = load_csv(csv_path)
            if error:
                raise Exception(f"Error loading CSV: {csv_path, error}")
            
            # verwijder de rijen waarvan de id al in de database zit
            df = df[~df["crm_Gebruikers_CRM_User_ID"].isin(existing_ids)]

            df = df.replace({np.nan: None})
            
            logger.info("Seeding inserting rows")
            progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)
            
            chunks = [df[i:i + BATCH_SIZE] for i in range(0, df.shape[0], BATCH_SIZE)]
            for chunk in chunks:
                gebruiker_data = []
                for _, row in chunk.iterrows(): 
                    p = Gebruiker(
                        GebruikerId=row["crm_Gebruikers_CRM_User_ID"],
                        BusinessUnitNaam=row["crm_Gebruikers_Business_Unit_Naam_"]
                    )
                    gebruiker_data.append(p)

                insert_gebruiker_data(gebruiker_data, session)
                progress_bar.update(len(gebruiker_data))

            progress_bar.close()

            # verplaats het csv bestand naar de "old" map voor reeds verwerkte bestanden
            move_csv_file(csv_path, old_csv_dir)

            logger.info(f"Number of new (non-duplicate) rows found in {csv_path}: {len(df)}")
    
    # als er geen nieuwe data is, dan is de lijst leeg
    if not gebruiker_data:
        logger.info("No new data was given. Data is up to date already.")
        
    session.close()