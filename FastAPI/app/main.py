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
df1 = pd.read_sql(query, engine)
df1.set_index('ContactPersoonId', inplace=True)
df1["rating"] = 5

query2 = """
    with pageview_count as(
        select p.PageTitle, count(p.ContactId) as count
        from Pageviews p
        group by p.PageTitle
    )
    select PageTitle, ContactId
    from Pageviews
    where Pagetitle in (select top (2500) PageTitle
    from pageview_count
    order by count desc)
"""
df2 = pd.read_sql(query2, engine)
df2.set_index('ContactId', inplace=True)
df2["rating"] = 1
df1_pivot = pd.pivot_table(df1, index='ContactPersoonId', columns=['Ondernemingsaard', 'Ondernemingstype', 'PrimaireActiviteit', 'Functie'], values='rating', fill_value = 0).astype('float16')
df2_pivot = pd.pivot_table(df2, index='ContactId', columns=['PageTitle'], values='rating', fill_value = 0).astype('float16')

df_pivot = pd.concat([df1_pivot, df2_pivot], axis=1, join='inner').astype('float16')
del [[ df1_pivot, df2_pivot]]

def calc(contact_id: str):
    select_contact = contact_id
    similarity_matrix = cosine_similarity(df_pivot, [df_pivot.loc[select_contact]]).reshape(1,-1)[0]
    similarities = df_pivot.index.join(similarity_matrix)
    similarities = pd.DataFrame({'ContactPersoonId':df_pivot.index, 'sim':similarity_matrix}).set_index('ContactPersoonId')
    similar_users = similarities[similarities['sim'] > 0.75].sort_values(by='sim', ascending=False)
    done_campaigns = df1.loc[df.index == select_contact]['CampagneId']
    similar_campagnes = df1[df1.index.isin(similar_users.index)].replace(0, np.nan).dropna(axis=1, how='all')
    similar_campagnes = similar_campagnes.drop_duplicates(subset=['CampagneId'])
    similar_campagnes = similar_campagnes[similar_campagnes['Startdatum'] > '2023-11-16']
    return similar_campagnes


app = FastAPI()


@app.get("/health")
def read_root():
    return {"health": "OK"}

@app.get("/contact/{contact_id}")
def contact_c(contact_id: str):
    return calc(contact_id)