from .base import Base

import logging
from sqlalchemy.orm import Mapped, mapped_column, sessionmaker, relationship
from sqlalchemy import String, Integer, ForeignKey, text
from sqlalchemy.dialects.mssql import BIT
from repository.main import get_engine, DATA_PATH
import pandas as pd
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
    
BATCH_SIZE = 10_000

logger = logging.getLogger(__name__)

class Contactfiche(Base):
    __tablename__ = "Contactfiche" 
    __table_args__ = {"extend_existing": True}
    #Id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)  # Id aanmaken want primary key is niet te vinden
    #edit: ContactPersoon is PK, want hiernaar wordt gerefereerd uit andere tables. Is ook uniek in de hele datafile. Kan wel interessant zijn voor DWH
    ContactpersoonId: Mapped[str] = mapped_column(String(255), nullable=False, primary_key=True)
    FunctieTitel: Mapped[str] = mapped_column(String(255), nullable=True)
    Status: Mapped[str] = mapped_column(String(50))
    VokaMedewerker: Mapped[BIT] = mapped_column(BIT)

    #FK
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
    


def insert_contactfiche_data(contactfiche_data, session):
    session.bulk_save_objects(contactfiche_data)
    session.commit()


def seed_contactfiche():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    logger.info("Reading CSV...")
    csv = DATA_PATH + "/Contact.csv"
    df = pd.read_csv(
        csv,
        delimiter=",",
        encoding="utf-8",
        keep_default_na=True,
        na_values=[""],
    )
    df = df.replace({np.nan: None})
    contactfiche_data = []
    logger.info("Seeding inserting rows")
    progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)
    
    for i, row in df.iterrows():
        p = Contactfiche(
            ContactpersoonId=row["crm_Contact_Contactpersoon"],
            AccountId=row["crm_Contact_Account"],
            FunctieTitel=row["crm_Contact_Functietitel"],
            PersoonId=row["crm_Contact_Persoon_ID"],
            Status=row["crm_Contact_Status"],
            VokaMedewerker=row["crm_Contact_Voka_medewerker"],
        )
        contactfiche_data.append(p)

        if len(contactfiche_data) >= BATCH_SIZE:
            insert_contactfiche_data(contactfiche_data, session)
            contactfiche_data = []
            progress_bar.update(BATCH_SIZE)

    if contactfiche_data:
        insert_contactfiche_data(contactfiche_data, session)
        progress_bar.update(len(contactfiche_data))

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