from .base import Base

import logging
from sqlalchemy.orm import Mapped, mapped_column, sessionmaker, relationship
from sqlalchemy import String
from repository.main import get_engine, DATA_PATH
import pandas as pd
import numpy as np
from tqdm import tqdm
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .info_en_klachten import InfoEnKlachten

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
    

def seed_gebruiker():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    
    logger.info("Reading CSV...")
    csv = DATA_PATH + '/Gebruikers.csv'
    df = pd.read_csv(csv, delimiter=",", encoding='utf-8-sig', keep_default_na=True, na_values=['']) # utf-8-sig is nodig om rare tekens te vermijden die er wel zijn bij utf-8
    
    df = df.replace({np.nan: None})
    
    gebruiker_data = []
    logger.info("Seeding inserting rows")
    progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)

    for _, row in df.iterrows():
        # print(row)
        p = Gebruiker(
            GebruikerId=row["crm_Gebruikers_CRM_User_ID"],
            BusinessUnitNaam=row["crm_Gebruikers_Business_Unit_Naam_"]
        )
        gebruiker_data.append(p)
        
        if len(gebruiker_data) >= BATCH_SIZE:
            insert_gebruiker_data(gebruiker_data, session)
            gebruiker_data = []
            progress_bar.update(BATCH_SIZE)

    if gebruiker_data:
        insert_gebruiker_data(gebruiker_data, session)
        progress_bar.update(len(gebruiker_data))