import logging
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData

from reposiroty.base import Base

logger = logging.getLogger(__name__)

load_dotenv()
DB_URL = os.getenv("DB_URL")
logger.info("Connecting to database...")  # IDK why dit niet logged
engine = create_engine(DB_URL)
conn = engine.connect()
metadata = MetaData()
logger.info("Connected")


def get_engine():
    return engine


# Import moeten hier door depenesie en anders worden de Classes ook niet ingeladen
# Voeg and adere seed imports hier onder toe
from reposiroty.pageviews import seed_pageviews
from reposiroty.sessie_inschrijving import seed_sessie_inschrijving
from reposiroty.sessie import seed_sessie
from reposiroty.inschrijving import seed_inschrijving


Base.metadata.reflect(engine)


def db_init():
    logger.info("Creating database and tables...")
    Base.metadata.create_all(engine)
    logger.info("Created tables:\n" + "\n".join(Base.metadata.tables.keys()))


# Voeg hier je seedfunctie toe
def db_seed():
    logger.info("Starting seeding...")
    seed_pageviews()
    seed_sessie_inschrijving()
    seed_sessie()
    seed_inschrijving()
