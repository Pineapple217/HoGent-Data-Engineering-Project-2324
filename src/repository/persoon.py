from .base import Base
from .functionalities import load_csv, move_csv_file

import logging
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column, relationship
from sqlalchemy import String, DateTime, Integer, Date
from sqlalchemy.orm import sessionmaker
from repository.main import get_engine, DATA_PATH
import os
import numpy as np
from tqdm import tqdm
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from .contactfiche import Contactfiche

BATCH_SIZE = 5_000

logger = logging.getLogger(__name__)


class Persoon(Base):
    __tablename__ = "Persoon"
    __table_args__ = {"extend_existing": True}
    PersoonId: Mapped[str] = mapped_column(String(100), nullable=False, primary_key=True)
    CRMPersoonsnr: Mapped[str] = mapped_column(Integer, nullable=True)
    RedenVanStatus: Mapped[int] = mapped_column(Integer)
    WebLogin: Mapped[str] = mapped_column(String(100), nullable=True)
    RegioAntwerpenWaasland: Mapped[int] = mapped_column(Integer)
    RegioBrusselHoofdstedelijkGewest: Mapped[int] = mapped_column(Integer)
    RegioLimburg: Mapped[int] = mapped_column(Integer)
    RegioMechelenKempen: Mapped[int] = mapped_column(Integer)
    RegioOostVlaanderen: Mapped[int] = mapped_column(Integer)
    RegioVlaamsBrabant: Mapped[int] = mapped_column(Integer)
    RegioVokaNationaal: Mapped[int] = mapped_column(Integer)
    RegioWestVlaanderen: Mapped[int] = mapped_column(Integer)
    ThemaDuurzaamheid: Mapped[int] = mapped_column(Integer)
    ThemaFinancieelFiscaal: Mapped[int] = mapped_column(Integer)
    ThemaInnovatie: Mapped[int] = mapped_column(Integer)
    ThemaInternationaalOndernemen: Mapped[int] = mapped_column(Integer)
    ThemaMobiliteit: Mapped[int] = mapped_column(Integer)
    ThemaOmgeving: Mapped[int] = mapped_column(Integer)
    ThemaSalesMarketingCommunicatie: Mapped[int] = mapped_column(Integer)
    ThemaStrategieEnAlgemeenManagement: Mapped[int] = mapped_column(Integer)
    ThemaTalent: Mapped[int] = mapped_column(Integer)
    ThemaWelzijn: Mapped[int] = mapped_column(Integer)
    TypeBevraging: Mapped[int] = mapped_column(Integer)
    TypeCommunitiesEnProjecten: Mapped[int] = mapped_column(Integer)
    TypeNetwerkevenementen: Mapped[int] = mapped_column(Integer)
    TypeNieuwsbrieven: Mapped[int] = mapped_column(Integer)
    TypeOpleidingen: Mapped[int] = mapped_column(Integer)
    TypePersberichtenBelangrijkeMeldingen: Mapped[int] = mapped_column(Integer)
    Marketingcommunicatie: Mapped[str] = mapped_column(String(50), nullable=True)

    # FK
    Contactfiche: Mapped["Contactfiche"] = relationship(back_populates="Persoon")


def insert_persoon_data(persoon_data, session):
    session.bulk_save_objects(persoon_data)
    session.commit()


#functie om alle id's te querien, zodat gelijke rijden niet appended worden
def get_existing_ids(session):
    return [result[0] for result in session.query(Persoon.PersoonId).all()]


def seed_persoon():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    
    existing_ids = get_existing_ids(session)

    #stel dirs voor old en new in en check of ze kloppen
    old_csv_dir = os.path.join(DATA_PATH, "old")
    new_csv_dir = os.path.join(DATA_PATH, "new")
    if not os.path.exists(old_csv_dir) or not os.path.exists(new_csv_dir):
        raise FileNotFoundError("The folders 'old' and 'new' must exist in the data folder")
    
    folder_new = new_csv_dir

    persoon_data = [] #1e initialize om te checken of er data bij komt
    for filename in os.listdir(folder_new): #check alle filenames in 'new'
        if filename == 'Persoon.csv': #hardcoded filename, zodat startswith niet fout kan lopen
            csv_path = os.path.join(folder_new, filename) #vul filepath aan met gevonden file
    
            logger.info(f"Reading CSV: {csv_path}")
            df, error = load_csv(csv_path) #load_csv uit functionalities.py, probeert met hardcoded delimiters en encodings een df te maken
            if error:
                raise Exception(f"Error loading CSV: {csv_path, error}")
            
            df = df.replace({np.nan: None})

            #filter df op alle contactpersonen die nog niet(~) bestaan, voor iterrows
            df = df[~df['crm_Persoon_persoon'].isin(existing_ids)]  

            yes_no_mapping = {'Nee': 0, 'Ja': 1}
            actief_inactief_mapping = {'Inactief': 0, 'Actief': 1}
            for column in df.columns:
                if "Mail" in column or "Type" in column:  
                    df[column] = df[column].map(yes_no_mapping).fillna(df[column])
                if "Reden_van_status" in column:  
                    df[column] = df[column].map(actief_inactief_mapping).fillna(df[column])


            # data in chunks steken
            chunks = [df[i:i + BATCH_SIZE] for i in range(0, df.shape[0], BATCH_SIZE)]
                #range van 0 tot aantal rijen in df, stap volgens batch size (hier 5,000)
                #maak lijst van chunks obv filtered df van i tot i + 5,000

            progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)
    

            for chunk in chunks:
                persoon_data = [] #2e initialize om data leeg te maken
                for _, row in chunk.iterrows():
                    p = Persoon(
                        PersoonId=row["crm_Persoon_persoon"],
                        CRMPersoonsnr=row["crm_Persoon_Persoonsnr_"],
                        RedenVanStatus=row["crm_Persoon_Reden_van_status"],
                        WebLogin=row["crm_Persoon_Web_Login"],
                        RegioAntwerpenWaasland=row["crm_Persoon_Mail_regio_Antwerpen_Waasland"],
                        RegioBrusselHoofdstedelijkGewest=row["crm_Persoon_Mail_regio_Brussel_Hoofdstedelijk_Gewest"],
                        RegioLimburg=row["crm_Persoon_Mail_regio_Limburg"],
                        RegioMechelenKempen=row["crm_Persoon_Mail_regio_Mechelen_Kempen"],
                        RegioOostVlaanderen=row["crm_Persoon_Mail_regio_Oost_Vlaanderen"],
                        RegioVlaamsBrabant=row["crm_Persoon_Mail_regio_Vlaams_Brabant"],
                        RegioVokaNationaal=row["crm_Persoon_Mail_regio_Voka_nationaal"],
                        RegioWestVlaanderen=row["crm_Persoon_Mail_regio_West_Vlaanderen"],
                        ThemaDuurzaamheid=row["crm_Persoon_Mail_thema_duurzaamheid"],
                        ThemaFinancieelFiscaal=row["crm_Persoon_Mail_thema_financieel_fiscaal"],
                        ThemaInnovatie=row["crm_Persoon_Mail_thema_innovatie"],
                        ThemaInternationaalOndernemen=row["crm_Persoon_Mail_thema_internationaal_ondernemen"],
                        ThemaMobiliteit=row["crm_Persoon_Mail_thema_mobiliteit"],
                        ThemaOmgeving=row["crm_Persoon_Mail_thema_omgeving"],
                        ThemaSalesMarketingCommunicatie=row["crm_Persoon_Mail_thema_sales_marketing_communicatie"],
                        ThemaStrategieEnAlgemeenManagement=row["crm_Persoon_Mail_thema_strategie_en_algemeen_management"],
                        ThemaTalent=row["crm_Persoon_Mail_thema_talent"],
                        ThemaWelzijn=row["crm_Persoon_Mail_thema_welzijn"],
                        TypeBevraging=row["crm_Persoon_Mail_type_Bevraging"],
                        TypeCommunitiesEnProjecten=row["crm_Persoon_Mail_type_communities_en_projecten"],
                        TypeNetwerkevenementen=row["crm_Persoon_Mail_type_netwerkevenementen"],
                        TypeNieuwsbrieven=row["crm_Persoon_Mail_type_nieuwsbrieven"],
                        TypeOpleidingen=row["crm_Persoon_Mail_type_opleidingen"],
                        TypePersberichtenBelangrijkeMeldingen=row["crm_Persoon_Mail_type_persberichten_belangrijke_meldingen"],
                        Marketingcommunicatie=row["crm_Persoon_Marketingcommunicatie"]
                    )
                    persoon_data.append(p)

                insert_persoon_data(persoon_data, session)
                progress_bar.update(len(persoon_data))

            progress_bar.close()

            move_csv_file(csv_path, old_csv_dir)

            logger.info(f"Number of new (non-duplicate) rows found in {csv_path}: {len(df)}")

    if not persoon_data:
        logger.info("No new data was given. Data is up to date already.")