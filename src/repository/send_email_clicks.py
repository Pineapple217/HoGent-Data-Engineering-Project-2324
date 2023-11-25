from .base import Base
from .functionalities import load_csv, move_csv_file

import logging
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column, relationship
from sqlalchemy import String, Integer, ForeignKey, text
from sqlalchemy.orm import sessionmaker
from repository.main import get_engine, DATA_PATH
import os
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
    Clicks            :Mapped[int] = mapped_column(Integer)

    # FK
    ContactId         :Mapped[str] = mapped_column(String(255), ForeignKey('Contactfiche.ContactpersoonId', use_alter=True), nullable=True)
    Contact           :Mapped["Contactfiche"] = relationship(back_populates="SendEmailClicks")
    
    EmailVersturenId  :Mapped[Optional[str]] = mapped_column(ForeignKey("Mailing.MailingId"), nullable=True)
    EmailVersturen    :Mapped[Optional["Mailing"]] = relationship(back_populates="SendClicks")
    

def insert_send_email_clicks_data(pageviews_data, session):
    session.bulk_save_objects(pageviews_data)
    session.commit()

def get_existing_ids(session):
    return [result[0] for result in session.query(SendEmailClicks.SentEmailId).all()]


def seed_send_email_clicks():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    existing_ids = get_existing_ids(session)

    old_csv_dir = os.path.join(DATA_PATH, "old")
    new_csv_dir = os.path.join(DATA_PATH, "new")
    if not os.path.exists(old_csv_dir) or not os.path.exists(new_csv_dir):
        raise FileNotFoundError("The folders 'old' and 'new' must exist in the data folder")
    
    folder_new = new_csv_dir

    send_email_clicks_data = []
    for filename in os.listdir(folder_new): #check alle filenames in 'new'
        if filename == 'CDI sent email clicks.csv':
            csv_path = os.path.join(folder_new, filename)
    
            logger.info(f"Reading CSV: {csv_path}")
            df, error = load_csv(csv_path)
            if error:
                raise Exception(f"Error loading CSV: {csv_path, error}")
            
            df = df.replace({np.nan: None})

            df = df[~df['crm_CDI_SentEmail_kliks_Sent_Email'].isin(existing_ids)]  

            # data in chunks steken
            chunks = [df[i:i + BATCH_SIZE] for i in range(0, df.shape[0], BATCH_SIZE)]

            progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)
            
            for chunk in chunks:
                send_email_clicks_data = []
                for _, row in chunk.iterrows():
                    p = SendEmailClicks(
                        SentEmailId       = row['crm_CDI_SentEmail_kliks_Sent_Email'],
                        Clicks            = row['crm_CDI_SentEmail_kliks_Clicks'],
                        ContactId         = row['crm_CDI_SentEmail_kliks_Contact'],
                        EmailVersturenId  = row['crm_CDI_SentEmail_kliks_E_mail_versturen'],
                    )
                    send_email_clicks_data.append(p)

                insert_send_email_clicks_data(send_email_clicks_data, session)
                progress_bar.update(len(send_email_clicks_data))

            progress_bar.close()

            move_csv_file(csv_path, old_csv_dir)

            logger.info(f"Number of new (non-duplicate) rows found in {csv_path}: {len(df)}")

    if not send_email_clicks_data:
        logger.info("No new data was given. Data is up to date already.")

    session.execute(text("""
        UPDATE SendEmailClicks
        SET SendEmailClicks.ContactId = NULL
        WHERE SendEmailClicks.ContactId
        NOT IN
        (SELECT ContactpersoonId FROM Contactfiche)
    """))
    session.commit()

    session.execute(text("""
        UPDATE SendEmailClicks
        SET SendEmailClicks.EmailVersturenId = NULL
        WHERE SendEmailClicks.EmailVersturenId
        NOT IN
        (SELECT MailingId FROM Mailing)
    """))
    session.commit()