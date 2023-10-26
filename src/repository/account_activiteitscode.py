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
    from .activiteitscode import Activiteitscode


BATCH_SIZE = 1_000

logger = logging.getLogger(__name__)

class AccountActiviteitscode(Base):
    __tablename__ = "AccountActiviteitscode"
    __table_args__ = {"extend_existing": True}
    Id: Mapped[str] = mapped_column(String(50), primary_key=True)
    Account: Mapped[str] = mapped_column(String(50), ForeignKey('Account.Id', use_alter=True))
    accountFK: Mapped["Account"] = relationship("Account", backref="FKAccountActiviteitscode")
    Activiteitscode: Mapped[str] = mapped_column(String(50), ForeignKey('Activiteitscode.Activiteitscode', use_alter=True))
    activiteitFK: Mapped["Activiteitscode"] = relationship("Activiteitscode", backref="FKActiviteitscodeAccount")


def insert_accountActiviteitscode_data(accountActiviteitscode_data, session):
    session.bulk_save_objects(accountActiviteitscode_data)
    session.commit()


def seed_account_activiteitscode():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    logger.info("Reading CSV...")
    csv = DATA_PATH + "/Account activiteitscode.csv"
    df = pd.read_csv(
        csv,
        delimiter=",",
        encoding="utf-8",
        keep_default_na=True,
        na_values=[""],
    )
    df = df.dropna(how="all", axis=0)
    df = df.replace({np.nan: None})

    accountActiviteitscode_data = []
    logger.info("Seeding inserting rows")
    progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)
    for i, row in df.iterrows():
        p = AccountActiviteitscode(
            Account=row["crm_Account_ActiviteitsCode_Account"],
            Activiteitscode=row["crm_Account_ActiviteitsCode_Activiteitscode"],
            Id=row["crm_Account_ActiviteitsCode_inf_account_inf_activiteitscodeId"],
        )
        accountActiviteitscode_data.append(p)

        if len(accountActiviteitscode_data) >= BATCH_SIZE:
            insert_accountActiviteitscode_data(accountActiviteitscode_data, session)
            accountActiviteitscode_data = []
            progress_bar.update(BATCH_SIZE)

    if accountActiviteitscode_data:
        insert_accountActiviteitscode_data(accountActiviteitscode_data, session)
        progress_bar.update(len(accountActiviteitscode_data))

    session.execute(text("""
        DELETE FROM AccountActiviteitscode
        WHERE Account NOT IN 
            (SELECT Id FROM Account);
    """)) #delete, want niet bruikbaar met null
    session.commit()

    session.execute(text("""
        DELETE FROM AccountActiviteitscode
        WHERE Activiteitscode NOT IN 
            (SELECT Activiteitscode FROM Activiteitscode);
    """)) #delete, want niet bruikbaar met null
    session.commit()