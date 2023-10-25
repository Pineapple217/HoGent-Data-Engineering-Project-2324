from .base import Base

import logging
from sqlalchemy.orm import Mapped, mapped_column, sessionmaker, relationship
from sqlalchemy import String, ForeignKey, text
from sqlalchemy.dialects.mssql import DATETIME2
from repository.main import get_engine, DATA_PATH
import pandas as pd
import numpy as np
from tqdm import tqdm
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from .gebruiker import Gebruiker
    
BATCH_SIZE = 10_000
CHUNK_SIZE = 10_000

logger = logging.getLogger(__name__)

class InfoEnKlachten(Base):
    __tablename__ = "InfoEnKlachten"
    __table_args__ = {"extend_existing": True}
    Aanvraag: Mapped[str] = mapped_column(String(50), primary_key=True)
    Account: Mapped[str] = mapped_column(String(50))
    Datum: Mapped[DATETIME2] = mapped_column(DATETIME2)
    DatumAfsluiting: Mapped[DATETIME2] = mapped_column(DATETIME2)
    Status: Mapped[str] = mapped_column(String(15))
    
    # FK
    EigenaarId: Mapped[Optional[str]] = mapped_column(ForeignKey("Gebruiker.Id", use_alter=True), nullable=True)
    Eigenaar: Mapped["Gebruiker"] = relationship(back_populates="InfoEnKlachten")


def insert_info_en_klachten_data(info_en_klachten_data, session):
    session.bulk_save_objects(info_en_klachten_data)
    session.commit()


def seed_info_en_klachten():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    logger.info("Reading CSV...")
    csv = DATA_PATH + "/Info en klachten.csv"
    df = pd.read_csv(csv, delimiter=",", encoding="utf-8", keep_default_na=True, na_values=[""])
    progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)
    chunks = pd.read_csv(csv, delimiter=",", encoding="utf-8", keep_default_na=True, na_values=[""], chunksize=CHUNK_SIZE)

    info_en_klachten_data = []
    date_format = "%d-%m-%Y %H:%M:%S"

    def create_info_en_klachten(chunk):
        chunk = chunk.replace({np.nan: None, "": None})
        chunk = chunk.drop_duplicates(subset=['crm_Info_en_Klachten_Aanvraag'])
        
        chunk["crm_Info_en_Klachten_Datum"] = pd.to_datetime(chunk["crm_Info_en_Klachten_Datum"], format=date_format)
        chunk["crm_Info_en_Klachten_Datum_afsluiting"] = pd.to_datetime(chunk["crm_Info_en_Klachten_Datum_afsluiting"], format=date_format)
        
        chunk_data = chunk.apply(lambda row: InfoEnKlachten(
            Aanvraag=row["crm_Info_en_Klachten_Aanvraag"],
            Account=row["crm_Info_en_Klachten_Account"],
            Datum=row["crm_Info_en_Klachten_Datum"],
            DatumAfsluiting=row["crm_Info_en_Klachten_Datum_afsluiting"],
            Status=row["crm_Info_en_Klachten_Status"],
            EigenaarId=row["crm_Info_en_Klachten_Eigenaar"]
        ), axis=1).tolist()
        info_en_klachten_data.extend(chunk_data)
        progress_bar.update(len(chunk_data))

    for chunk in chunks:
        info_en_klachten_data.clear()
        create_info_en_klachten(chunk)
        insert_info_en_klachten_data(info_en_klachten_data, session)

    session.execute(text("""
        UPDATE InfoEnKlachten
        SET InfoEnKlachten.EigenaarId = NULL
        WHERE InfoEnKlachten.EigenaarId
        NOT IN
        (SELECT Id FROM Gebruiker)
    """))
    session.commit()