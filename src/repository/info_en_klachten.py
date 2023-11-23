from .base import Base
from .functionalities import load_csv, move_csv_file

import logging
from sqlalchemy.orm import Mapped, mapped_column, sessionmaker, relationship
from sqlalchemy import String, ForeignKey, text
from sqlalchemy.dialects.mssql import DATETIME2
from repository.main import get_engine, DATA_PATH
import os
import pandas as pd
import numpy as np
from tqdm import tqdm
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from .gebruiker import Gebruiker
    from .account import Account
    
BATCH_SIZE = 10_000

logger = logging.getLogger(__name__)


class InfoEnKlachten(Base):
    __tablename__ = "InfoEnKlachten"
    __table_args__ = {"extend_existing": True}
    AanvraagId: Mapped[str] = mapped_column(String(50), primary_key=True)
    Datum: Mapped[DATETIME2] = mapped_column(DATETIME2)
    DatumAfsluiting: Mapped[DATETIME2] = mapped_column(DATETIME2)
    Status: Mapped[str] = mapped_column(String(15))
    
    # FK
    EigenaarId: Mapped[Optional[str]] = mapped_column(ForeignKey("Gebruiker.GebruikerId", use_alter=True), nullable=True)
    Eigenaar: Mapped["Gebruiker"] = relationship(back_populates="InfoEnKlachten")
    
    AccountId: Mapped[Optional[str]] = mapped_column(ForeignKey("Account.AccountId", use_alter=True), nullable=True)
    Account: Mapped["Account"] = relationship(back_populates="InfoEnKlachten")


def insert_info_en_klachten_data(info_en_klachten_data, session):
    session.bulk_save_objects(info_en_klachten_data)
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
    return [result[0] for result in session.query(InfoEnKlachten.AanvraagId).all()]


def seed_info_en_klachten():
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
    info_en_klachten_data = []
    
    for filename in os.listdir(folder_new):
        if filename == 'Info en klachten.csv':
            csv_path = os.path.join(folder_new, filename)
            logger.info(f"Reading CSV: {csv_path}")
            df, error = load_csv(csv_path)
            if error:
                raise Exception(f"Error loading CSV: {csv_path, error}")
            
            # verwijder de rijen waarvan de id al in de database zit
            df = df[~df["crm_Info_en_Klachten_Aanvraag"].isin(existing_ids)]
            
            df = df.replace({np.nan: None})
            df = df.replace({"": None})
            
            # duplicaten van primary keys in één bepaalde csv
            df = df.drop_duplicates(subset=['crm_Info_en_Klachten_Aanvraag'])
            
            df["crm_Info_en_Klachten_Datum"] = pd.to_datetime(df["crm_Info_en_Klachten_Datum"], format="%d-%m-%Y %H:%M:%S")
            df["crm_Info_en_Klachten_Datum_afsluiting"] = pd.to_datetime(df["crm_Info_en_Klachten_Datum_afsluiting"], format="%d-%m-%Y %H:%M:%S")
    
            logger.info("Seeding inserting rows")
            progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)
            
            # data in chunks steken
            chunks = [df[i:i + BATCH_SIZE] for i in range(0, df.shape[0], BATCH_SIZE)]
            for chunk in chunks:
                info_en_klachten_data = []
                for _, row in chunk.iterrows(): 
                    p = InfoEnKlachten(
                        AanvraagId=row["crm_Info_en_Klachten_Aanvraag"],
                        AccountId=row["crm_Info_en_Klachten_Account"],
                        Datum=row["crm_Info_en_Klachten_Datum"],
                        DatumAfsluiting=row["crm_Info_en_Klachten_Datum_afsluiting"],
                        Status=row["crm_Info_en_Klachten_Status"],
                        EigenaarId=row["crm_Info_en_Klachten_Eigenaar"],
                    )
                    info_en_klachten_data.append(p)

                insert_info_en_klachten_data(info_en_klachten_data, session)
                progress_bar.update(len(info_en_klachten_data))

            progress_bar.close()

            # verplaats het csv bestand naar de "old" map voor reeds verwerkte bestanden
            move_csv_file(csv_path, old_csv_dir)

            logger.info(f"Number of new (non-duplicate) rows found in {csv_path}: {len(df)}")
    
    # als er geen nieuwe data is, dan is de lijst leeg
    if not info_en_klachten_data:
        logger.info("No new data was given. Data is up to date already.")
    
    session.execute(text("""
        UPDATE InfoEnKlachten
        SET InfoEnKlachten.EigenaarId = NULL
        WHERE InfoEnKlachten.EigenaarId
        NOT IN
        (SELECT GebruikerId FROM Gebruiker)
    """))
    session.commit()

    session.execute(text("""
        UPDATE InfoEnKlachten
        SET InfoEnKlachten.AccountId = NULL
        WHERE InfoEnKlachten.AccountId
        NOT IN
        (SELECT AccountId FROM Account)
    """))
    session.commit()
    
    session.close()