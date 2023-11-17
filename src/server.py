import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sklearn.metrics.pairwise import cosine_similarity

from fastapi import FastAPI

load_dotenv()
DB_URL = os.getenv("DB_URL")
engine = create_engine(DB_URL)
query = """
    select c.ContactPersoonId, i.CampagneId, i.CampagneNaam, a.Ondernemingsaard, a.Ondernemingstype, a.PrimaireActiviteit, f.Naam as Functie
    from Contactfiche c
    join Account a on a.AccountId = c.AccountId
    join Inschrijving i on i.ContactficheId = c.ContactPersoonId
    join ContactficheFunctie cf on cf.ContactpersoonId = c.ContactPersoonId
    join Functie f on f.FunctieId = cf.FunctieId
    where CampagneId is not null;
"""
df = pd.read_sql(query, engine)
df.set_index('ContactPersoonId', inplace=True)
df["rating"] = 1
df_pivot = pd.pivot_table(df, index='ContactPersoonId', columns=['Ondernemingsaard', 'Ondernemingstype', 'PrimaireActiviteit', 'Functie'], values='rating', fill_value = 0)


def calc(contact_id: str):
    select_contact = contact_id
    similarity_matrix = cosine_similarity(df_pivot, [df_pivot.loc[select_contact]]).reshape(1,-1)[0]
    similarities = df_pivot.index.join(similarity_matrix)
    similarities = pd.DataFrame({'ContactPersoonId':df_pivot.index, 'sim':similarity_matrix}).set_index('ContactPersoonId')
    similar_users = similarities[similarities['sim'] > 0.75].sort_values(by='sim', ascending=False)
    done_campaigns = df.loc[df.index == select_contact]['CampagneId']
    similar_campagnes = df[df.index.isin(similar_users.index)].replace(0, np.nan).dropna(axis=1, how='all')
    similar_campagnes_not_done = similar_campagnes[~similar_campagnes['CampagneId'].isin(done_campaigns)][['CampagneId', 'CampagneNaam']]
    similar_campagnes_not_done.set_index('CampagneId', inplace=True)
    similar_campagnes_not_done.drop_duplicates(inplace=True)
    return similar_campagnes_not_done


server = FastAPI()


@server.get("/health")
def read_root():
    return {"health": "OK"}

@server.get("/contact/{contact_id}")
def contact_c(contact_id: str):
    return calc(contact_id)