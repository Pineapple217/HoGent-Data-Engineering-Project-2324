from .base import Base
from .functionalities import load_csv, move_csv_file

import logging
from sqlalchemy.orm import Mapped, mapped_column, sessionmaker, relationship
from sqlalchemy import String, Date, ForeignKey, text
from repository.main import get_engine, DATA_PATH
import os
import pandas as pd
import numpy as np
from tqdm import tqdm
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .contactfiche import Contactfiche
    from .afspraak_vereist_contact import AfspraakVereistContact
    from .afspraak_account import AfspraakAccount

BATCH_SIZE = 10_000

logger = logging.getLogger(__name__)


class AfspraakContact(Base):
    __tablename__ = "AfspraakContact"
    __table_args__ = {"extend_existing": True}
    AfspraakId: Mapped[str] = mapped_column(String(255), primary_key=True)
    Thema: Mapped[str] = mapped_column(String(255), nullable=True)
    Subthema: Mapped[str] = mapped_column(String(255), nullable=True)
    Onderwerp: Mapped[str] = mapped_column(String(255), nullable=True)
    Einddatum: Mapped[Date] = mapped_column(Date)
    KeyPhrases: Mapped[str] = mapped_column(String(3000), nullable=True)
    
    # FK
    ContactId: Mapped[str] = mapped_column(String(255), ForeignKey('Contactfiche.ContactpersoonId', use_alter=True), nullable=True)
    Contact: Mapped["Contactfiche"] = relationship(back_populates="AfspraakContact")

    AfspraakVereistContact: Mapped["AfspraakVereistContact"] = relationship(back_populates="Afspraak")
    AfspraakAccount: Mapped["AfspraakAccount"] = relationship(back_populates="Afspraak")


def insert_AfspraakContact_data(AfspraakContact_data, session):
    session.bulk_save_objects(AfspraakContact_data)
    session.commit()

#functie om alle id's te querien, zodat gelijke rijen niet appended worden
def get_existing_ids(session):
    return [result[0] for result in session.query(AfspraakContact.AfspraakId).all()]

def seed_afspraak_contact():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    #query bestaande id's
    existing_ids = get_existing_ids(session)
    
    #stel dirs voor old en new in en check of ze kloppen
    old_csv_dir = os.path.join(DATA_PATH, "old")
    new_csv_dir = os.path.join(DATA_PATH, "new")
    if not os.path.exists(old_csv_dir) or not os.path.exists(new_csv_dir):
        raise FileNotFoundError("The folders 'old' and 'new' must exist in the data folder")
    
    folder_new = new_csv_dir

    afspraakcontact_data = []
    for filename in os.listdir(folder_new): #check alle filenames in 'new'
        if filename == 'Afspraak betreft contact_cleaned.csv': #hardcoded filename, zodat startswith niet fout kan lopen
            csv_path = os.path.join(folder_new, filename) #vul filepath aan met gevonden file
    
            logger.info(f"Reading CSV: {csv_path}")
            df, error = load_csv(csv_path) #load_csv uit functionalities.py, probeert met hardcoded delimiters en encodings een df te maken
            if error:
                raise Exception(f"Error loading CSV: {csv_path, error}")
            
            df = df.replace({np.nan: None})
            df = df.drop_duplicates()
            
            df["crm_Afspraak_BETREFT_CONTACTFICHE_Eindtijd"] = pd.to_datetime(df["crm_Afspraak_BETREFT_CONTACTFICHE_Eindtijd"], format="%d-%m-%Y")

            #filter df op alle id's die nog niet(~) bestaan, voor iterrows
            df = df[~df['crm_Afspraak_BETREFT_CONTACTFICHE_Afspraak'].isin(existing_ids)]  

            # data in chunks steken
            chunks = [df[i:i + BATCH_SIZE] for i in range(0, df.shape[0], BATCH_SIZE)]
                #range van 0 tot aantal rijen in df, stap volgens batch size (hier 5,000)
                #maak lijst van chunks obv filtered df van i tot i + 5,000

            progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)

            for chunk in chunks:
                afspraakcontact_data = []
                for _, row in chunk.iterrows():
                    p = AfspraakContact(
                        AfspraakId=row["crm_Afspraak_BETREFT_CONTACTFICHE_Afspraak"],
                        Thema=row["crm_Afspraak_BETREFT_CONTACTFICHE_Thema"],
                        Subthema=row["crm_Afspraak_BETREFT_CONTACTFICHE_Subthema"],
                        Onderwerp=row["crm_Afspraak_BETREFT_CONTACTFICHE_Onderwerp"],
                        ContactId=row["crm_Afspraak_BETREFT_CONTACTFICHE_Betreft_id"],
                        Einddatum=row["crm_Afspraak_BETREFT_CONTACTFICHE_Eindtijd"],
                        KeyPhrases=row["crm_Afspraak_BETREFT_CONTACTFICHE_KeyPhrases"]
                    )
                    afspraakcontact_data.append(p)

                insert_AfspraakContact_data(afspraakcontact_data, session)
                progress_bar.update(len(afspraakcontact_data))

            progress_bar.close()

            move_csv_file(csv_path, old_csv_dir)

            logger.info(f"Number of new (non-duplicate) rows found in {csv_path}: {len(df)}")

    if not afspraakcontact_data:
        logger.info("No new data was given. Data is up to date already.")

    session.execute(text("""
        UPDATE AfspraakContact
        SET AfspraakContact.ContactId = NULL
        WHERE AfspraakContact.ContactId
        NOT IN
        (SELECT ContactpersoonId FROM Contactfiche)
    """))
    session.commit()

    

