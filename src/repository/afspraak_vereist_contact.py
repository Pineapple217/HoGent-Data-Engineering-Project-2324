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
    from .afspraak_contact import AfspraakContact

BATCH_SIZE = 10_000

logger = logging.getLogger(__name__)


class AfspraakVereistContact(Base):
    __tablename__ = "AfspraakVereistContact"
    __table_args__ = {'extend_existing': True}
    AfspraakVereistContactId: Mapped[int] = mapped_column(primary_key=True, autoincrement=True) # zelf toegevoegd, tabel heeft geen primary key

    # FK
    AfspraakId: Mapped[str] = mapped_column(String(255), ForeignKey('AfspraakContact.AfspraakId'))
    Afspraak: Mapped["AfspraakContact"] = relationship(back_populates="AfspraakVereistContact")

    VereistContactId: Mapped[str] = mapped_column(String(255),ForeignKey('Contactfiche.ContactpersoonId'), primary_key=True)
    Contact: Mapped["Contactfiche"] = relationship(back_populates="AfspraakVereistContact")
    

def insert_AfspraakVereistContact_data(AfspraakVereistContact_data, session):
    session.bulk_save_objects(AfspraakVereistContact_data)
    session.commit()


def seed_afspraak_vereist_contact():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    
    logger.info("Reading CSV...")
    csv = DATA_PATH + '/Activiteit vereist contact.csv'
    df = pd.read_csv(csv, delimiter=",", encoding='utf-8-sig', keep_default_na=True, na_values=[''])
    
    df = df.drop_duplicates()
    df = df.replace({np.nan: None})
    
    AfspraakVereistContact_data = []
    logger.info("Seeding inserting rows")
    progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)
    for _, row in df.iterrows():
        avc = AfspraakVereistContact(
            AfspraakId=row["crm_ActiviteitVereistContact_ActivityId"],
            VereistContactId=row["crm_ActiviteitVereistContact_ReqAttendee"],
        )
        AfspraakVereistContact_data.append(avc)
        
        if len(AfspraakVereistContact_data) >= BATCH_SIZE:
            insert_AfspraakVereistContact_data(AfspraakVereistContact_data, session)
            AfspraakVereistContact_data = []
            progress_bar.update(BATCH_SIZE)

    if AfspraakVereistContact_data:
        insert_AfspraakVereistContact_data(AfspraakVereistContact_data, session)
        progress_bar.update(len(AfspraakVereistContact_data))

    session.execute(text("""
        DELETE FROM AfspraakVereistContact
        WHERE VereistContactId NOT IN 
            (SELECT ContactpersoonId FROM Contactfiche);
    """)) #delete, want niet bruikbaar met null
    session.commit()

    session.execute(text("""
        DELETE FROM AfspraakVereistContact
        WHERE AfspraakId NOT IN 
            (SELECT AfspraakId FROM AfspraakContact);
    """)) #delete, want niet bruikbaar met null
    session.commit()