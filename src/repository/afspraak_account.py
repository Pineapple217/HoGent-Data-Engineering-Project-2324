from .base import Base
from .functionalities import load_csv, move_csv_file

import logging
from sqlalchemy.orm import Mapped, mapped_column, sessionmaker, relationship
from sqlalchemy import String, ForeignKey, text
from repository.main import get_engine, DATA_PATH
import os
import pandas as pd
import numpy as np
from tqdm import tqdm
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .account import Account
    from .afspraak_contact import AfspraakContact

BATCH_SIZE = 10_000

logger = logging.getLogger(__name__)


# Data in commentaar is data die al in AfspraakContact staat, maar gejoined is. Anders hebben we duplicate data.
class AfspraakAccount(Base):
    __tablename__ = "AfspraakAccount"
    __table_args__ = {"extend_existing": True}
    AfspraakAccountId: Mapped[int] = mapped_column(primary_key=True, autoincrement=True) # zelf toegevoegd, tabel heeft geen primary key
    AfspraakId: Mapped[str] = mapped_column(String(255), ForeignKey('AfspraakContact.AfspraakId'))
    Afspraak: Mapped["AfspraakContact"] = relationship(back_populates="AfspraakAccount")
    #Thema: Mapped[str] = mapped_column(String(255), nullable=True)
    #Subthema: Mapped[str] = mapped_column(String(255), nullable=True)
    #Onderwerp: Mapped[str] = mapped_column(String(255), nullable=True)
    #Einddatum: Mapped[Date] = mapped_column(Date, nullable=True)
    AccountId: Mapped[str] = mapped_column(String(50), ForeignKey('Account.AccountId'))
    Account: Mapped["Account"] = relationship(back_populates="AfspraakAccount")
    #KeyPhrases: Mapped[str] = mapped_column(String(3000), nullable=True)
    
    
def insert_afspraak_account_data(afspraak_account_data, session):
    session.bulk_save_objects(afspraak_account_data)
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


# geen filtering op bestaande ids in de database wegens autoincrement primary key


def seed_afspraak_account():
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
    afspraak_account_data = []
    
    # enkel de csv bestanden in de "new" map worden ingelezen, de bestanden in de "old" map zijn reeds verwerkt
    for filename in os.listdir(folder_new):
        if filename == 'Afspraak_account_gelinkt_cleaned.csv':
            csv_path = os.path.join(folder_new, filename)
            logger.info(f"Reading CSV: {csv_path}")
            df, error = load_csv(csv_path)
            if error:
                raise Exception(f"Error loading CSV: {csv_path, error}")
            
            df = df.drop_duplicates()
            
            df = df.replace({np.nan: None})
            
            df["crm_Afspraak_ACCOUNT_GELINKT_Eindtijd"] = pd.to_datetime(df["crm_Afspraak_ACCOUNT_GELINKT_Eindtijd"], format="%d-%m-%Y")
            
            logger.info("Seeding inserting rows")
            progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)
            
            chunks = [df[i:i + BATCH_SIZE] for i in range(0, df.shape[0], BATCH_SIZE)]
            for chunk in chunks:
                afspraak_account_data = []
                for _, row in chunk.iterrows(): 
                    p = AfspraakAccount(
                        AfspraakId=row["crm_Afspraak_ACCOUNT_GELINKT_Afspraak"],
                        #Thema=row["crm_Afspraak_ACCOUNT_GELINKT_Thema"],
                        #Subthema=row["crm_Afspraak_ACCOUNT_GELINKT_Subthema"],
                        #Onderwerp=row["crm_Afspraak_ACCOUNT_GELINKT_Onderwerp"],
                        #Einddatum=row["crm_Afspraak_ACCOUNT_GELINKT_Eindtijd"],
                        AccountId=row["crm_Afspraak_ACCOUNT_GELINKT_Account"],
                        #KeyPhrases=row["crm_Afspraak_ACCOUNT_GELINKT_KeyPhrases"]
                    )
                    afspraak_account_data.append(p)

                insert_afspraak_account_data(afspraak_account_data, session)
                progress_bar.update(len(afspraak_account_data))

            progress_bar.close()

            # verplaats het csv bestand naar de "old" map voor reeds verwerkte bestanden
            move_csv_file(csv_path, old_csv_dir)

            logger.info(f"Number of new (non-duplicate) rows found in {csv_path}: {len(df)}")
    
    # als er geen nieuwe data is, dan is de lijst leeg
    if not afspraak_account_data:
        logger.info("No new data was given. Data is up to date already.")
    
    session.execute(text("""
        DELETE FROM AfspraakAccount
        WHERE AfspraakId NOT IN 
            (SELECT AfspraakId FROM AfspraakContact);
    """)) #delete, want niet bruikbaar met null
    session.commit()

    session.execute(text("""
        DELETE FROM AfspraakAccount
        WHERE AccountId NOT IN 
            (SELECT AccountId FROM Account);
    """)) #delete, want niet bruikbaar met null
    session.commit()
    
    session.close()