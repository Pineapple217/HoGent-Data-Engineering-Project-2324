from .base import Base
from .functionalities import load_csv, move_csv_file

import logging
from sqlalchemy.orm import Mapped, mapped_column, sessionmaker, relationship
from sqlalchemy import Integer, FLOAT, ForeignKey, text
from sqlalchemy.dialects.mssql import DATETIME2
from repository.main import get_engine, DATA_PATH
import os
import pandas as pd
import numpy as np
from tqdm import tqdm
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from .account import Account

BATCH_SIZE = 25_000

logger = logging.getLogger(__name__)


class AccountFinancieleData(Base):
    __tablename__ = "AccountFinancieleData"
    __table_args__ = {"extend_existing": True}
    AccountFinancieleDataId: Mapped[int] = mapped_column(primary_key=True, autoincrement=True) # zelf toegevoegd, tabel heeft geen primary key
    Boekjaar: Mapped[int] = mapped_column(Integer)
    AantalMaanden: Mapped[int] = mapped_column(Integer, nullable=True)
    ToegevoegdeWaarde: Mapped[FLOAT] = mapped_column(FLOAT, nullable=True)
    FTE: Mapped[FLOAT] = mapped_column(FLOAT, nullable=True)
    GewijzigdOp: Mapped[DATETIME2] = mapped_column(DATETIME2)
    
    # FK
    OndernemingId: Mapped[Optional[str]] = mapped_column(ForeignKey("Account.AccountId", use_alter=True), nullable=True)
    Onderneming: Mapped["Account"] = relationship(back_populates="AccountFinancieleData")
    
    
def insert_account_financiele_data_data(account_financiele_data_data, session):
    session.bulk_save_objects(account_financiele_data_data)
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


def seed_account_financiele_data():
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
    account_financiele_data_data = []
    
    # enkel de csv bestanden in de "new" map worden ingelezen, de bestanden in de "old" map zijn reeds verwerkt
    for filename in os.listdir(folder_new):
        if filename == 'Account financiële data.csv':
            csv_path = os.path.join(folder_new, filename)
            logger.info(f"Reading CSV: {csv_path}")
            df, error = load_csv(csv_path)
            if error:
                raise Exception(f"Error loading CSV: {csv_path, error}")
            
            df["crm_FinancieleData_Gewijzigd_op"] = pd.to_datetime(df["crm_FinancieleData_Gewijzigd_op"], format="%d-%m-%Y %H:%M:%S")

            df['crm_FinancieleData_FTE'] = df['crm_FinancieleData_FTE'].str.replace(',', '.')
            df['crm_FinancieleData_Toegevoegde_waarde'] = df['crm_FinancieleData_Toegevoegde_waarde'].str.replace(',', '.')
            
            df['crm_FinancieleData_FTE'] = pd.to_numeric(df['crm_FinancieleData_FTE'],errors='coerce')
            df['crm_FinancieleData_Toegevoegde_waarde'] = pd.to_numeric(df['crm_FinancieleData_Toegevoegde_waarde'],errors='coerce')
            df['crm_FinancieleData_Aantal_maanden'] = pd.to_numeric(df['crm_FinancieleData_Aantal_maanden'],errors='coerce')
            
            df = df.replace({np.nan: None})
            
            logger.info("Seeding inserting rows")
            progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)
            
            chunks = [df[i:i + BATCH_SIZE] for i in range(0, df.shape[0], BATCH_SIZE)]
            for chunk in chunks:
                account_financiele_data_data = []
                for _, row in chunk.iterrows(): 
                    p = AccountFinancieleData(
                        OndernemingId=row["crm_FinancieleData_OndernemingID"],
                        Boekjaar=row["crm_FinancieleData_Boekjaar"],
                        AantalMaanden=row["crm_FinancieleData_Aantal_maanden"],
                        ToegevoegdeWaarde=row["crm_FinancieleData_Toegevoegde_waarde"],
                        FTE=row["crm_FinancieleData_FTE"],
                        GewijzigdOp=row["crm_FinancieleData_Gewijzigd_op"],
                    )
                    account_financiele_data_data.append(p)

                insert_account_financiele_data_data(account_financiele_data_data, session)
                progress_bar.update(len(account_financiele_data_data))

            progress_bar.close()

            # verplaats het csv bestand naar de "old" map voor reeds verwerkte bestanden
            move_csv_file(csv_path, old_csv_dir)

            logger.info(f"Number of new (non-duplicate) rows found in {csv_path}: {len(df)}")
    
    # als er geen nieuwe data is, dan is de lijst leeg
    if not account_financiele_data_data:
        logger.info("No new data was given. Data is up to date already.")
    
    session.execute(text("""
        UPDATE AccountFinancieleData
        SET AccountFinancieleData.OndernemingId = NULL
        WHERE AccountFinancieleData.OndernemingId
        NOT IN
        (SELECT AccountId FROM Account)
    """))
    session.commit()
    
    session.close()