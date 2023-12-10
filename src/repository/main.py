import logging
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, text

from repository.base import Base

logger = logging.getLogger(__name__)

load_dotenv()
DB_URL = os.getenv("DB_URL")
DATA_PATH = os.getenv("DATA_PATH")
SQL_FOLDER_PATH = './src/repository/views'
logger.info("Connecting to database...")  # IDK why dit niet logged
engine = create_engine(DB_URL)
conn = engine.connect()
metadata = MetaData()
metadata.bind = engine
logger.info("Connected")


def get_engine():
    return engine

def drop_fk():
    query = text(f"""
        DECLARE @sql nvarchar(max) = N'';

        ;WITH x AS 
        (
        SELECT DISTINCT obj = 
            QUOTENAME(OBJECT_SCHEMA_NAME(parent_object_id)) + '.' 
            + QUOTENAME(OBJECT_NAME(parent_object_id)) 
        FROM sys.foreign_keys
        )
        SELECT @sql += N'ALTER TABLE ' + obj + N' NOCHECK CONSTRAINT ALL;
        ' FROM x;

        EXEC sys.sp_executesql @sql;
    """)
    
    conn.execute(query)
    conn.commit()

def enable_fk():
    query = text(f"""
        DECLARE @sql nvarchar(max) = N'';

        ;WITH x AS 
        (
        SELECT DISTINCT obj = 
            QUOTENAME(OBJECT_SCHEMA_NAME(parent_object_id)) + '.' 
            + QUOTENAME(OBJECT_NAME(parent_object_id)) 
        FROM sys.foreign_keys
        )
        SELECT @sql += N'ALTER TABLE ' + obj + N' WITH CHECK CHECK CONSTRAINT ALL;
        ' FROM x;

        EXEC sys.sp_executesql @sql;
    """)
    
    conn.execute(query)
    conn.commit()

# Import moeten hier door dependencies, anders worden de classes niet ingeladen
# Voeg andere seed imports hier onder toe
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
from repository.afspraak_vereist_contact import seed_afspraak_vereist_contact
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

def db_build():
    db_init()
    db_seed()
    db_views()


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
    drop_fk()
    # Finale ordening obv hoeveel FK's verwijzen naar hun PK
    seed_persoon()                
    seed_account()                #3
    seed_contactfiche()           #3
    seed_afspraak_contact()       #2
    seed_mailing()                #2
    seed_functie()                #1
    seed_activiteitscode()        #1
    seed_campagne()               #1
    seed_inschrijving()           #1
    seed_sessie()                 #1
    seed_visits()                 #1
    seed_gebruiker()              #1
    seed_send_email_clicks()      
    seed_web_content()
    seed_pageviews()
    seed_sessie_inschrijving()   
    seed_info_en_klachten()
    seed_account_financiele_data()
    seed_afspraak_account()
    seed_afspraak_vereist_contact()
    seed_account_activiteitscode()
    seed_contactfiche_functie()
    enable_fk()

def remove_old_views():
    query = text(f"""
        DECLARE @viewName NVARCHAR(255);
        DECLARE viewCursor CURSOR FOR
            SELECT TABLE_SCHEMA + '.' + TABLE_NAME
            FROM INFORMATION_SCHEMA.VIEWS;

        OPEN viewCursor;
        FETCH NEXT FROM viewCursor INTO @viewName;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            EXEC('DROP VIEW ' + @viewName);

            FETCH NEXT FROM viewCursor INTO @viewName;
        END

        CLOSE viewCursor;
        DEALLOCATE viewCursor;
    """)
    conn.execute(query)
    conn.commit()

def db_views():
    logger.info("Removing all old views")
    remove_old_views()
    for file_name in os.listdir(SQL_FOLDER_PATH):
        if file_name.endswith('.sql'):
            file_path = os.path.join(SQL_FOLDER_PATH, file_name)

            with open(file_path, 'r') as file:
                sql_query_string = file.read()

            query = text(sql_query_string)

            try:
                conn.execute(query)
                conn.commit()
                logger.info(f"Script {file_name} executed successfully.")
            except Exception as e:
                logger.error(f"Error executing script {file_name}: {str(e)}")