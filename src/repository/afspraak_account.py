from .base import Base

import logging
from sqlalchemy.orm import Mapped, mapped_column, sessionmaker, relationship
from sqlalchemy import String, ForeignKey, text
from repository.main import get_engine, DATA_PATH
import pandas as pd
import numpy as np
from tqdm import tqdm
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .account import Account
    from .afspraak_contact import AfspraakContact


BATCH_SIZE = 10_000

logger = logging.getLogger(__name__)

#data in commentaar is data die al in AfspraakContact staat, maar gejoined is. Ander duplicate data.
class AfspraakAccount(Base):
    __tablename__ = "AfspraakAccount"
    __table_args__ = {"extend_existing": True}
    Id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True) # zelf toegevoegd, tabel heeft geen primary key
    AfspraakId: Mapped[str] = mapped_column(String(255), ForeignKey('AfspraakContact.AfspraakId'))
    afspraak: Mapped["AfspraakContact"] = relationship(back_populates="AfspraakAccount")
    #Thema: Mapped[str] = mapped_column(String(255), nullable=True)
    #Subthema: Mapped[str] = mapped_column(String(255), nullable=True)
    #Onderwerp: Mapped[str] = mapped_column(String(255), nullable=True)
    #Einddatum: Mapped[Date] = mapped_column(Date, nullable=True)
    AccountId: Mapped[str] = mapped_column(String(50), ForeignKey('Account.AccountId'))
    account: Mapped["Account"] = relationship(back_populates="AfspraakAccount")
    #KeyPhrases: Mapped[str] = mapped_column(String(3000), nullable=True)
    

def insert_AfspraakAccount_data(AfspraakAccount_data, session):
    session.bulk_save_objects(AfspraakAccount_data)
    session.commit()


def seed_afspraak_account():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    logger.info("Reading CSV...")
    csv = DATA_PATH + '/Afspraak_account_gelinkt_cleaned.csv'
    df = pd.read_csv(csv, delimiter=",", encoding='utf-8-sig', keep_default_na=True, na_values=[''])
    df = df.drop_duplicates()
    df = df.replace({np.nan: None})
    df["crm_Afspraak_ACCOUNT_GELINKT_Eindtijd"] = pd.to_datetime(
        df["crm_Afspraak_ACCOUNT_GELINKT_Eindtijd"], format="%d-%m-%Y"
    )
    AfspraakAccount_data = []
    logger.info("Seeding inserting rows")
    progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)
    for _, row in df.iterrows():
        aa = AfspraakAccount(
            AfspraakId=row["crm_Afspraak_ACCOUNT_GELINKT_Afspraak"],
            #Thema=row["crm_Afspraak_ACCOUNT_GELINKT_Thema"],
            #Subthema=row["crm_Afspraak_ACCOUNT_GELINKT_Subthema"],
            #Onderwerp=row["crm_Afspraak_ACCOUNT_GELINKT_Onderwerp"],
            #Einddatum=row["crm_Afspraak_ACCOUNT_GELINKT_Eindtijd"],
            AccountId=row["crm_Afspraak_ACCOUNT_GELINKT_Account"],
            #KeyPhrases=row["crm_Afspraak_ACCOUNT_GELINKT_KeyPhrases"]
        )

        AfspraakAccount_data.append(aa)
        
        if len(AfspraakAccount_data) >= BATCH_SIZE:
            insert_AfspraakAccount_data(AfspraakContact_data, session)
            AfspraakContact_data = []
            progress_bar.update(BATCH_SIZE)

    if AfspraakAccount_data:
        insert_AfspraakAccount_data(AfspraakAccount_data, session)
        progress_bar.update(len(AfspraakAccount_data))

        
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


