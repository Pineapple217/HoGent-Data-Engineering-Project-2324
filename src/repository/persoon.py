from .base import Base

import logging
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column, relationship
from sqlalchemy import String, DateTime, Integer, Date
from sqlalchemy.orm import sessionmaker
from repository.main import get_engine, DATA_PATH
import pandas as pd
import numpy as np
from tqdm import tqdm
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from .contactfiche import Contactfiche

BATCH_SIZE = 10_000

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


def seed_persoon():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    
    logger.info("Reading CSV...")
    csv = DATA_PATH + '/Persoon.csv'
    df = pd.read_csv(csv, delimiter=",", encoding='utf-8-sig', keep_default_na=True, na_values=[''])
    
    df = df.replace({np.nan: None})
    
    yes_no_mapping = {'Nee': 0, 'Ja': 1}
    actief_inactief_mapping = {'Inactief': 0, 'Actief': 1}
    
    persoon_data = []
    logger.info("Seeding inserting rows")
    progress_bar = tqdm(total=len(df), unit=" rows", unit_scale=True)
    for _, row in df.iterrows():
        p = Persoon(
            PersoonId=row["crm_Persoon_persoon"],
            CRMPersoonsnr=row["crm_Persoon_Persoonsnr_"],
            RedenVanStatus=actief_inactief_mapping.get(row["crm_Persoon_Reden_van_status"]),
            WebLogin=row["crm_Persoon_Web_Login"],
            RegioAntwerpenWaasland=yes_no_mapping.get(row["crm_Persoon_Mail_regio_Antwerpen_Waasland"], None),
            RegioBrusselHoofdstedelijkGewest=yes_no_mapping.get(row["crm_Persoon_Mail_regio_Brussel_Hoofdstedelijk_Gewest"], None),
            RegioLimburg=yes_no_mapping.get(row["crm_Persoon_Mail_regio_Limburg"], None),
            RegioMechelenKempen=yes_no_mapping.get(row["crm_Persoon_Mail_regio_Mechelen_Kempen"], None),
            RegioOostVlaanderen=yes_no_mapping.get(row["crm_Persoon_Mail_regio_Oost_Vlaanderen"], None),
            RegioVlaamsBrabant=yes_no_mapping.get(row["crm_Persoon_Mail_regio_Vlaams_Brabant"], None),
            RegioVokaNationaal=yes_no_mapping.get(row["crm_Persoon_Mail_regio_Voka_nationaal"], None),
            RegioWestVlaanderen=yes_no_mapping.get(row["crm_Persoon_Mail_regio_West_Vlaanderen"], None),
            ThemaDuurzaamheid=yes_no_mapping.get(row["crm_Persoon_Mail_thema_duurzaamheid"], None),
            ThemaFinancieelFiscaal=yes_no_mapping.get(row["crm_Persoon_Mail_thema_financieel_fiscaal"], None),
            ThemaInnovatie=yes_no_mapping.get(row["crm_Persoon_Mail_thema_innovatie"], None),
            ThemaInternationaalOndernemen=yes_no_mapping.get(row["crm_Persoon_Mail_thema_internationaal_ondernemen"], None),
            ThemaMobiliteit=yes_no_mapping.get(row["crm_Persoon_Mail_thema_mobiliteit"], None),
            ThemaOmgeving=yes_no_mapping.get(row["crm_Persoon_Mail_thema_omgeving"], None),
            ThemaSalesMarketingCommunicatie=yes_no_mapping.get(row["crm_Persoon_Mail_thema_sales_marketing_communicatie"], None),
            ThemaStrategieEnAlgemeenManagement=yes_no_mapping.get(row["crm_Persoon_Mail_thema_strategie_en_algemeen_management"], None),
            ThemaTalent=yes_no_mapping.get(row["crm_Persoon_Mail_thema_talent"], None),
            ThemaWelzijn=yes_no_mapping.get(row["crm_Persoon_Mail_thema_welzijn"], None),
            TypeBevraging=yes_no_mapping.get(row["crm_Persoon_Mail_type_Bevraging"], None),
            TypeCommunitiesEnProjecten=yes_no_mapping.get(row["crm_Persoon_Mail_type_communities_en_projecten"], None),
            TypeNetwerkevenementen=yes_no_mapping.get(row["crm_Persoon_Mail_type_netwerkevenementen"], None),
            TypeNieuwsbrieven=yes_no_mapping.get(row["crm_Persoon_Mail_type_nieuwsbrieven"], None),
            TypeOpleidingen=yes_no_mapping.get(row["crm_Persoon_Mail_type_opleidingen"], None),
            TypePersberichtenBelangrijkeMeldingen=yes_no_mapping.get(row["crm_Persoon_Mail_type_persberichten_belangrijke_meldingen"], None),
            Marketingcommunicatie=row["crm_Persoon_Marketingcommunicatie"]
        )
        persoon_data.append(p)
        
        if len(persoon_data) >= BATCH_SIZE:
            insert_persoon_data(persoon_data, session)
            persoon_data = []
            progress_bar.update(BATCH_SIZE)

    if persoon_data:
        insert_persoon_data(persoon_data, session)
        progress_bar.update(len(persoon_data))