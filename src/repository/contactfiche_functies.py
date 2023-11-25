from .base import Base
from .functionalities import load_csv, move_csv_file

import logging
from sqlalchemy.orm import Mapped, mapped_column, sessionmaker, relationship
from sqlalchemy import Integer, ForeignKey, text
from repository.main import get_engine, DATA_PATH
import os
import pandas as pd
import numpy as np
from tqdm import tqdm
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from .contactfiche import Contactfiche
    from .functie import Functie

BATCH_SIZE = 10_000

logger = logging.getLogger(__name__)


class ContactficheFunctie(Base):
    __tablename__ = "ContactficheFunctie" 
    __table_args__ = {"extend_existing": True}
    ContactficheFunctieId: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    
    # FK
    ContactpersoonId: Mapped[Optional[str]] = mapped_column(ForeignKey("Contactfiche.ContactpersoonId", use_alter=True), nullable=True)
    Contactpersoon: Mapped["Contactfiche"] = relationship(back_populates="ContactficheFunctie")
    
    FunctieId: Mapped[Optional[str]] = mapped_column(ForeignKey("Functie.FunctieId", use_alter=True), nullable=True)
    Functie: Mapped["Functie"] = relationship(back_populates="ContactficheFunctie")
    

def insert_contactfiche_functie_data(contactfiche_functie_data, session):
    session.bulk_save_objects(contactfiche_functie_data)
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


# filtering op beide kolommen (tussentabel) om te voorkomen dat er dubbele rijen worden ingevoegd
def get_existing_contactpersoon_functie_ids(session):
    contactpersoon_ids = [result[0] for result in session.query(ContactficheFunctie.ContactpersoonId).all()]
    functie_ids = [result[0] for result in session.query(ContactficheFunctie.FunctieId).all()]
    return contactpersoon_ids, functie_ids


def seed_contactfiche_functie():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    
    # pad naar de csv bestanden, deze moeten in de mappen "old" en "new" zitten
    old_csv_dir = os.path.join(DATA_PATH, "old")
    new_csv_dir = os.path.join(DATA_PATH, "new")
    
    # check of de mappen "old" en "new" bestaan
    if not os.path.exists(old_csv_dir) or not os.path.exists(new_csv_dir):
        raise FileNotFoundError("The folders 'old' and 'new' must exist in the data folder")

    folder_new = new_csv_dir
    contactfiche_functie_data = []
    
    # haal de bestaande ids op
    existing_contactpersoon_ids, existing_functie_ids = get_existing_contactpersoon_functie_ids(session)
    
    # enkel de csv bestanden in de "new" map worden ingelezen, de bestanden in de "old" map zijn reeds verwerkt
    for filename in os.listdir(folder_new):
        if filename == 'Contact functie.csv':
            csv_path = os.path.join(folder_new, filename)
            logger.info(f"Reading CSV: {csv_path}")
            df, error = load_csv(csv_path)
            if error:
                raise Exception(f"Error loading CSV: {csv_path, error}")
            
            # dataframe met de bestaande combinaties van contactpersoon en functie
            existing_ids_df = pd.DataFrame({'crm_ContactFunctie_Contactpersoon': existing_contactpersoon_ids, 'crm_ContactFunctie_Functie': existing_functie_ids})
            df = df.merge(existing_ids_df, on=['crm_ContactFunctie_Contactpersoon', 'crm_ContactFunctie_Functie'], how='left', indicator=True)
            # dataframe met enkel de nieuwe rijen (waar contactpersoon en functie een unieke combinatie vormen)
            df = df[df['_merge'] == 'left_only'].drop('_merge', axis=1)
            
            # data cleaning
            df = df.dropna(how='all', axis=0)
            df = df.replace({np.nan: None})
            
            logger.info("Seeding inserting rows")
            progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)
            
            chunks = [df[i:i + BATCH_SIZE] for i in range(0, df.shape[0], BATCH_SIZE)]
            for chunk in chunks:
                contactfiche_functie_data = []
                for _, row in chunk.iterrows(): 
                    p = ContactficheFunctie(
                        ContactpersoonId=row["crm_ContactFunctie_Contactpersoon"],
                        FunctieId=row["crm_ContactFunctie_Functie"],
                    )
                    contactfiche_functie_data.append(p)

                insert_contactfiche_functie_data(contactfiche_functie_data, session)
                progress_bar.update(len(contactfiche_functie_data))

            progress_bar.close()

            # verplaats het csv bestand naar de "old" map voor reeds verwerkte bestanden
            move_csv_file(csv_path, old_csv_dir)

            logger.info(f"Number of new (non-duplicate) rows found in {csv_path}: {len(df)}")
    
    # als er geen nieuwe data is, dan is de lijst leeg
    if not contactfiche_functie_data:
        logger.info("No new data was given. Data is up to date already.")
    
    session.execute(text("""
        DELETE FROM ContactficheFunctie
        WHERE ContactpersoonId NOT IN 
            (SELECT ContactpersoonId FROM Contactfiche);
    """)) #delete, want niet bruikbaar met null
    session.commit()

    session.execute(text("""
        DELETE FROM ContactficheFunctie
        WHERE FunctieId NOT IN 
            (SELECT FunctieId FROM Functie);
    """)) #delete, want niet bruikbaar met null
    session.commit()
    
    session.close()