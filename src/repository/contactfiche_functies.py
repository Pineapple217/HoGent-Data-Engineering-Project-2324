from .base import Base

import logging
from sqlalchemy.orm import Mapped, mapped_column, sessionmaker, relationship
from sqlalchemy import String, Integer, ForeignKey, text
from repository.main import get_engine, DATA_PATH
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
    Id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ContactpersoonId: Mapped[str] = mapped_column(String(255), ForeignKey('Contactfiche.ContactpersoonId', use_alter=True), nullable=True)
    contactFK: Mapped["Contactfiche"] = relationship("Contactfiche", backref="FKContactficheFunctieContact")
    
    # FK
    FunctieId: Mapped[Optional[str]] = mapped_column(ForeignKey("Functie.FunctieId", use_alter=True), nullable=True)
    Functie: Mapped["Functie"] = relationship(back_populates="ContactficheFunctie")

def insert_contactfiche_functie_data(contactfiche_functie_data, session):
    session.bulk_save_objects(contactfiche_functie_data)
    session.commit()

def seed_contactfiche_functie():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    logger.info("Reading CSV...")
    csv = DATA_PATH + "/Contact functie.csv"
    df = pd.read_csv(
        csv,
        delimiter=",",
        encoding="utf-8",
        keep_default_na=True,
        na_values=[""]
    )
    df = df.dropna(how='all', axis=0)
    df = df.replace({np.nan: None})
    contactfiche_functie_data = []
    logger.info("Seeding inserting rows")
    progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)
    
    for i, row in df.iterrows():
        p = ContactficheFunctie(
            ContactpersoonId=row["crm_ContactFunctie_Contactpersoon"],
            FunctieId=row["crm_ContactFunctie_Functie"],
        )
        contactfiche_functie_data.append(p)

        if len(contactfiche_functie_data) >= BATCH_SIZE:
            insert_contactfiche_functie_data(contactfiche_functie_data, session)
            contactfiche_functie_data = []
            progress_bar.update(BATCH_SIZE)

    if contactfiche_functie_data:
        insert_contactfiche_functie_data(contactfiche_functie_data, session)
        progress_bar.update(len(contactfiche_functie_data))

    
    session.execute(text("""
        DELETE FROM ContactficheFunctie
        WHERE ContactPersoonId NOT IN 
            (SELECT ContactPersoonId FROM Contactfiche);
    """)) #delete, want niet bruikbaar met null
    session.commit()

    session.execute(text("""
        DELETE FROM ContactficheFunctie
        WHERE FunctieId NOT IN 
            (SELECT FunctieId FROM Functie);
    """)) #delete, want niet bruikbaar met null
    session.commit()