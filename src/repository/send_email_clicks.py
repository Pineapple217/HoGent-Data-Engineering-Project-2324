from .base import Base

import logging
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column, relationship
from sqlalchemy import String, Integer, ForeignKey, text
from sqlalchemy.orm import sessionmaker
from repository.main import get_engine, DATA_PATH
import pandas as pd
import numpy as np
from typing import Optional
from tqdm import tqdm

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .mailing import Mailing
    from .contactfiche import Contactfiche


BATCH_SIZE = 10_000

logger = logging.getLogger(__name__)


class SendEmailClicks(Base):
    __tablename__ = "SendEmailClicks"
    __table_args__ = {"extend_existing": True}
    SentEmailId       :Mapped[str] = mapped_column(String(50), primary_key=True)
    Clicks          :Mapped[int] = mapped_column(Integer)

    ContactId         :Mapped[str] = mapped_column(String(255), ForeignKey('Contactfiche.ContactpersoonId', use_alter=True), nullable=True)
    Contact: Mapped["Contactfiche"] = relationship(back_populates="SendEmailClicks")
    EmailVersturenId:Mapped[Optional[str]] = mapped_column(ForeignKey("Mailing.MailingId"), nullable=True)
    EmailVersturen  :Mapped[Optional["Mailing"]] = relationship(back_populates="SendClicks")
    


def insert_send_email_clicks_data(pageviews_data, session):
    session.bulk_save_objects(pageviews_data)
    session.commit()


def seed_send_email_clicks():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    logger.info("Reading CSV...")
    csv = DATA_PATH + "/CDI sent email clicks.csv"
    df = pd.read_csv(
        csv, delimiter=",", encoding="utf-8", keep_default_na=True, na_values=[""]
    )
    df = df.replace({np.nan: None})
    # Sommige lege waardes worden als NaN ingelezeno
    # NaN mag niet in een varchar

    csv_fk = DATA_PATH + "/CDI mailing.csv"
    df_fk = pd.read_csv(
        csv_fk, delimiter=",", encoding="utf-8", keep_default_na=True, na_values=[""]
    )
    df_fk = df_fk.replace({np.nan: None})
    valid_mailings = df_fk['crm_CDI_Mailing_Mailing'].tolist()

    send_email_clicks_data = []
    logger.info("Seeding inserting rows")
    progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)
    for _, row in df.iterrows():
        email_id = row['crm_CDI_SentEmail_kliks_E_mail_versturen']
        p = SendEmailClicks(
            SentEmailId       = row['crm_CDI_SentEmail_kliks_Sent_Email'],
            Clicks          = row['crm_CDI_SentEmail_kliks_Clicks'],
            ContactId         = row['crm_CDI_SentEmail_kliks_Contact'],
            EmailVersturenId  = email_id,
        )
        if email_id not in valid_mailings:
            p.EmailVersturenId = None
        send_email_clicks_data.append(p)

        if len(send_email_clicks_data) >= BATCH_SIZE:
            insert_send_email_clicks_data(send_email_clicks_data, session)
            send_email_clicks_data = []
            progress_bar.update(BATCH_SIZE)

    # Insert any remaining data
    if send_email_clicks_data:
        insert_send_email_clicks_data(send_email_clicks_data, session)
        progress_bar.update(len(send_email_clicks_data))

    session.execute(text("""
        UPDATE SendEmailClicks
        SET SendEmailClicks.ContactId = NULL
        WHERE SendEmailClicks.ContactId
        NOT IN
        (SELECT ContactPersoonId FROM Contactfiche)
    """))
    session.commit()