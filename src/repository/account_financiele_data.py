from .base import Base

import logging
from sqlalchemy.orm import Mapped, mapped_column, sessionmaker, relationship
from sqlalchemy import String, Integer, FLOAT, ForeignKey, text
from sqlalchemy.dialects.mssql import DATETIME2
from repository.main import get_engine, DATA_PATH
import pandas as pd
import numpy as np
from tqdm import tqdm
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from .account import Account

BATCH_SIZE = 10_000

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


def seed_account_financiele_data():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    
    logger.info("Reading CSV...")
    csv = DATA_PATH + "/Account financiële data.csv"
    df = pd.read_csv(csv, delimiter=",", encoding="utf-8", keep_default_na=True, na_values=[""])

    df["crm_FinancieleData_Gewijzigd_op"] = pd.to_datetime(df["crm_FinancieleData_Gewijzigd_op"], format="%d-%m-%Y %H:%M:%S")
    df['crm_FinancieleData_FTE'] = df['crm_FinancieleData_FTE'].str.replace(',', '.')
    df['crm_FinancieleData_Toegevoegde_waarde'] = df['crm_FinancieleData_Toegevoegde_waarde'].str.replace(',', '.')
    df['crm_FinancieleData_FTE'] = pd.to_numeric(df['crm_FinancieleData_FTE'],errors='coerce')
    df['crm_FinancieleData_Toegevoegde_waarde'] = pd.to_numeric(df['crm_FinancieleData_Toegevoegde_waarde'],errors='coerce')
    df['crm_FinancieleData_Aantal_maanden'] = pd.to_numeric(df['crm_FinancieleData_Aantal_maanden'],errors='coerce')
    
    df = df.replace({np.nan: None})
    
    account_financiele_data_data = []
    logger.info("Seeding inserting rows")
    progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)
    
    for _, row in df.iterrows():
        p = AccountFinancieleData(
            OndernemingId=row["crm_FinancieleData_OndernemingID"],
            Boekjaar=row["crm_FinancieleData_Boekjaar"],
            AantalMaanden=row["crm_FinancieleData_Aantal_maanden"],
            ToegevoegdeWaarde=row["crm_FinancieleData_Toegevoegde_waarde"],
            FTE=row["crm_FinancieleData_FTE"],
            GewijzigdOp=row["crm_FinancieleData_Gewijzigd_op"],
        )
        account_financiele_data_data.append(p)

        if len(account_financiele_data_data) >= BATCH_SIZE:
            insert_account_financiele_data_data(account_financiele_data_data, session)
            account_financiele_data_data = []
            progress_bar.update(BATCH_SIZE)

    if account_financiele_data_data:
        insert_account_financiele_data_data(account_financiele_data_data, session)
        progress_bar.update(len(account_financiele_data_data))

    session.execute(text("""
        UPDATE AccountFinancieleData
        SET AccountFinancieleData.OndernemingId = NULL
        WHERE AccountFinancieleData.OndernemingId
        NOT IN
        (SELECT AccountId FROM Account)
    """))
    session.commit()