from .base import Base

import logging
from sqlalchemy.orm import Mapped, mapped_column, sessionmaker, relationship
from sqlalchemy import String, Date, ForeignKey, text
from repository.main import get_engine, DATA_PATH
import pandas as pd
import numpy as np
from tqdm import tqdm
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .contactfiche import Contactfiche

BATCH_SIZE = 10_000

logger = logging.getLogger(__name__)

class AfspraakContact(Base):
    __tablename__ = "AfspraakContact"
    __table_args__ = {"extend_existing": True}
    AfspraakID: Mapped[str] = mapped_column(String(255), primary_key=True)
    Thema: Mapped[str] = mapped_column(String(255), nullable=True)
    Subthema: Mapped[str] = mapped_column(String(255), nullable=True)
    Onderwerp: Mapped[str] = mapped_column(String(255), nullable=True)
    ContactID: Mapped[str] = mapped_column(String(255), ForeignKey('Contactfiche.ContactPersoon', use_alter=True), nullable=True)
    contact: Mapped["Contactfiche"] = relationship("Contactfiche", backref="FKAfspraakContact")
    Einddatum: Mapped[Date] = mapped_column(Date)
    KeyPhrases: Mapped[str] = mapped_column(String(3000)    , nullable=True)
    

def insert_AfspraakContact_data(AfspraakContact_data, session):
    session.bulk_save_objects(AfspraakContact_data)
    session.commit()


def seed_afspraak_contact():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    logger.info("Reading CSV...")
    csv = DATA_PATH + '/Afspraak betreft contact_cleaned.csv'
    df = pd.read_csv(csv, delimiter=",", encoding='utf-8-sig', keep_default_na=True, na_values=[''])
    df = df.drop_duplicates()
    df = df.replace({np.nan: None})
    df["crm_Afspraak_BETREFT_CONTACTFICHE_Eindtijd"] = pd.to_datetime(
        df["crm_Afspraak_BETREFT_CONTACTFICHE_Eindtijd"], format="%d-%m-%Y"
    )
    
    AfspraakContact_data = []
    logger.info("Seeding inserting rows")
    progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)
    for _, row in df.iterrows():
        ac = AfspraakContact(
            AfspraakID=row["crm_Afspraak_BETREFT_CONTACTFICHE_Afspraak"],
            Thema=row["crm_Afspraak_BETREFT_CONTACTFICHE_Thema"],
            Subthema=row["crm_Afspraak_BETREFT_CONTACTFICHE_Subthema"],
            Onderwerp=row["crm_Afspraak_BETREFT_CONTACTFICHE_Onderwerp"],
            ContactID=row["crm_Afspraak_BETREFT_CONTACTFICHE_Betreft_id"],
            Einddatum=row["crm_Afspraak_BETREFT_CONTACTFICHE_Eindtijd"],
            KeyPhrases=row["crm_Afspraak_BETREFT_CONTACTFICHE_KeyPhrases"]
        )

        AfspraakContact_data.append(ac)
        
        if len(AfspraakContact_data) >= BATCH_SIZE:
            insert_AfspraakContact_data(AfspraakContact_data, session)
            AfspraakContact_data = []
            progress_bar.update(BATCH_SIZE)

    if AfspraakContact_data:
        insert_AfspraakContact_data(AfspraakContact_data, session)
        progress_bar.update(len(AfspraakContact_data))

    session.execute(text("""
        UPDATE AfspraakContact
        SET AfspraakContact.ContactID = NULL
        WHERE AfspraakContact.ContactID
        NOT IN
        (SELECT ContactPersoon FROM Contactfiche)
    """))
    session.commit()

    

