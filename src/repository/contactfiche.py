from .base import Base
from .functionalities import load_csv, move_csv_file

import logging
from sqlalchemy.orm import Mapped, mapped_column, sessionmaker, relationship
from sqlalchemy import String, Integer, ForeignKey, text
from repository.main import get_engine, DATA_PATH
import os
import numpy as np
from tqdm import tqdm
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .account import Account
    from .persoon import Persoon
    from .inschrijving import Inschrijving
    from .send_email_clicks import SendEmailClicks
    from .afspraak_contact import AfspraakContact
    from .afspraak_vereist_contact import AfspraakVereistContact
    from .pageviews import Pageview
    from .visits import Visit
    
BATCH_SIZE = 25_000

logger = logging.getLogger(__name__)


class Contactfiche(Base):
    __tablename__ = "Contactfiche" 
    __table_args__ = {"extend_existing": True}
    #Id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)  # Id aanmaken want primary key is niet te vinden
    # Edit: ContactPersoon is PK, want hiernaar wordt gerefereerd uit andere tables. Het is ook uniek in de hele datafile. Kan wel interessant zijn voor DWH
    ContactpersoonId: Mapped[str] = mapped_column(String(255), nullable=False, primary_key=True)
    FunctieTitel: Mapped[str] = mapped_column(String(255), nullable=True)
    Status: Mapped[str] = mapped_column(String(50))
    VokaMedewerker: Mapped[int] = mapped_column(Integer)

    # FK
    AccountId: Mapped[str] = mapped_column(String(50), ForeignKey('Account.AccountId'), nullable=True)
    Account: Mapped["Account"] = relationship(back_populates="Contactfiche")

    PersoonId: Mapped[str] = mapped_column(String(100), ForeignKey('Persoon.PersoonId'), nullable=True)
    Persoon: Mapped["Persoon"] = relationship(back_populates="Contactfiche")

    Inschrijving: Mapped["Inschrijving"] = relationship(back_populates="Contactfiche")
    SendEmailClicks      :Mapped["SendEmailClicks"] = relationship(back_populates="Contact")
    AfspraakContact: Mapped["AfspraakContact"] = relationship(back_populates="Contact")
    AfspraakVereistContact: Mapped["AfspraakVereistContact"] = relationship(back_populates="Contact")
    Pageviews: Mapped["Pageview"] = relationship(back_populates="Contact")
    Visit: Mapped["Visit"] = relationship(back_populates="Contact")
    ContactficheFunctie: Mapped["ContactficheFunctie"] = relationship(back_populates="Contactpersoon")


def insert_contactfiche_data(contactfiche_data, session):
    session.bulk_save_objects(contactfiche_data)
    session.commit()


#functie om alle id's te querien, zodat gelijke rijden niet appended worden
def get_existing_ids(session):
    return [result[0] for result in session.query(Contactfiche.ContactpersoonId).all()]

def seed_contactfiche():
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

    contactfiche_data = []
    for filename in os.listdir(folder_new): #check alle filenames in 'new'
        if filename == 'Contact.csv': #hardcoded filename, zodat startswith niet fout kan lopen
            csv_path = os.path.join(folder_new, filename) #vul filepath aan met gevonden file
    
            logger.info(f"Reading CSV: {csv_path}")
            df, error = load_csv(csv_path) #load_csv uit functionalities.py, probeert met hardcoded delimiters en encodings een df te maken
            if error:
                raise Exception(f"Error loading CSV: {csv_path, error}")
            
            df = df.replace({np.nan: None})

            #filter df op alle contactpersonen die nog niet(~) bestaan, voor iterrows
            df = df[~df['crm_Contact_Contactpersoon'].isin(existing_ids)]  

            # data in chunks steken
            chunks = [df[i:i + BATCH_SIZE] for i in range(0, df.shape[0], BATCH_SIZE)]
                #range van 0 tot aantal rijen in df, stap volgens batch size (hier 5,000)
                #maak lijst van chunks obv filtered df van i tot i + 5,000

            progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)

            for chunk in chunks:
                contactfiche_data = []
                for _, row in chunk.iterrows():
                    p = Contactfiche(
                            ContactpersoonId=row["crm_Contact_Contactpersoon"],
                            AccountId=row["crm_Contact_Account"],
                            FunctieTitel=row["crm_Contact_Functietitel"],
                            PersoonId=row["crm_Contact_Persoon_ID"],
                            Status=row["crm_Contact_Status"],
                            VokaMedewerker=row["crm_Contact_Voka_medewerker"]
                    )
                    contactfiche_data.append(p)

                insert_contactfiche_data(contactfiche_data, session)
                progress_bar.update(len(contactfiche_data))

            progress_bar.close()

            move_csv_file(csv_path, old_csv_dir)

            logger.info(f"Number of new (non-duplicate) rows found in {csv_path}: {len(df)}")

    if not contactfiche_data:
        logger.info("No new data was given. Data is up to date already.")

    session.execute(text("""
        UPDATE Contactfiche
        SET Contactfiche.AccountId = NULL
        WHERE Contactfiche.AccountId
        NOT IN
        (SELECT AccountId FROM Account)
    """))
    session.commit()

    session.execute(text("""
        UPDATE Contactfiche
        SET Contactfiche.PersoonId = NULL
        WHERE Contactfiche.PersoonId
        NOT IN
        (SELECT PersoonId FROM Persoon)
    """))
    session.commit()