import logging
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData

from repository.base import Base

logger = logging.getLogger(__name__)

load_dotenv()
DB_URL = os.getenv("DB_URL")
DATA_PATH = os.getenv("DATA_PATH")
logger.info("Connecting to database...")  # IDK why dit niet logged
engine = create_engine(DB_URL)
conn = engine.connect()
metadata = MetaData()
logger.info("Connected")


def get_engine():
    return engine


# Import moeten hier door depenesie en anders worden de Classes ook niet ingeladen
# Voeg and adere seed imports hier onder toe
from repository.pageviews import seed_pageviews
from repository.mailing import seed_mailing
from repository.send_email_clicks import seed_send_email_clicks
from repository.web_content import seed_web_content
from repository.visits import seed_visits
from repository.sessie_inschrijving import seed_sessie_inschrijving
from repository.sessie import seed_sessie
from repository.inschrijving import seed_inschrijving
from repository.gebruiker import seed_gebruiker
from repository.info_en_klachten import seed_info_en_klachten
from repository.account import seed_account
from repository.campagne import seed_campagne
from repository.persoon import seed_persoon
from repository.afspraak_contact import seed_afspraak_contact
from repository.afspraak_account import seed_afspraak_account
from repository.activiteit_contact import seed_activiteit_contact
from repository.contactfiche_functies import seed_contactfiche_functie
from repository.contactfiche import seed_contactfiche
from repository.account_activiteitscode import seed_account_activiteitscode
from repository.functie import seed_functie
from repository.activiteitscode import seed_activiteitscode
from repository.account_financiele_data import seed_account_financiele_data


Base.metadata.reflect(engine)


def db_rebuild():
    db_drop()
    db_init()
    db_seed()


def db_drop():
    metadata.drop_all(bind=engine)
    logger.info("Tables dropped")


def db_init():
    logger.info("Creating database and tables...")
    Base.metadata.create_all(engine)
    logger.info("Created tables:\n" + "\n".join(Base.metadata.tables.keys()))


# Voeg hier je seedfunctie toe
def db_seed():
    logger.info("Starting seeding...")

    # seed_pageviews()
    seed_mailing()
    seed_send_email_clicks()
    seed_web_content()
    # seed_visits()
    seed_sessie_inschrijving()
    seed_sessie()
    seed_inschrijving()
    seed_gebruiker()
    seed_info_en_klachten()
    seed_account()
    seed_account_financiele_data()
    seed_campagne()
    seed_persoon()
    seed_afspraak_contact()
    seed_afspraak_account()
    seed_activiteit_contact()
    seed_account_activiteitscode()
    seed_activiteitscode()
    seed_contactfiche_functie()
    seed_contactfiche()
    seed_functie()
