import logging
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import declarative_base, sessionmaker
from repository.base import Base

logger = logging.getLogger(__name__)

load_dotenv()
DB_URL = os.getenv("DB_URL")
DATA_PATH = os.getenv("DATA_PATH")
engine = create_engine(DB_URL)
conn = engine.connect()
metadata = MetaData()

Base = declarative_base()
Base.metadata.reflect(engine)
table_names = [
    "Pageviews",
    "Mailing",
    "SendEmailClicks",
    "WebContent",
    "Visits",
    "SessieInschrijving",
    "Sessie",
    "Inschrijving",
    "Gebruiker",
    "InfoEnKlachten",
    "Account",
    "Persoon",
    "AfspraakContact",
    "AfspraakAccount",
    "AfspraakVereistContact",
    "ContactficheFunctie",
    "Contactfiche",
    "AccountActiviteitscode",
    "Functie",
    "Activiteitscode",
    "AccountFinancieleData",
    "Campagne",
]

table_names = Base.metadata.tables.keys()
Session = sessionmaker(bind=engine)
session = Session()


def db_test():
    logger.info("Testing database...")

    for table_name in table_names:
        emp_table = Table(table_name, Base.metadata, autoload_with=engine)
        query = emp_table.select().limit(1)
        result = session.execute(query)
        print("")
        logger.info(" 1 row from table: " + table_name)
        print(result.fetchall())
        session.commit()

    logger.info("Database test succesfull")

    session.close()
