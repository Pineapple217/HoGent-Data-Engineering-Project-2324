import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, text
from sklearn.metrics.pairwise import cosine_similarity

from fastapi import FastAPI

load_dotenv()
DB_URL = os.getenv("DB_URL")
engine = create_engine(DB_URL)
conn = engine.connect()
metadata = MetaData()
metadata.reflect(bind=engine)
query = text('''
            select c.ContactPersoonId, i.CampagneId, i.CampagneNaam, ca.Startdatum, a.Ondernemingsaard, a.Ondernemingstype, a.PrimaireActiviteit, f.Naam as Functie
            from Contactfiche c
            join Account a on a.AccountId = c.AccountId
            join Inschrijving i on i.ContactficheId = c.ContactPersoonId
            join Campagne ca on ca.CampagneId = i.CampagneId
            join ContactficheFunctie cf on cf.ContactpersoonId = c.ContactPersoonId
            join Functie f on f.FunctieId = cf.FunctieId
            where i.CampagneId is not null and a.status = 'actief';
            ''')
result = conn.execute(query)

#convert to dataframe
df_omschrijving = pd.DataFrame(result.fetchall())
df_omschrijving.set_index('ContactPersoonId', inplace=True)
df_omschrijving["rating"] = 5

query2 = text('''
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
              ''')

result2 = conn.execute(query2)
df_pageviews = pd.DataFrame(result2.fetchall())
df_pageviews.set_index('ContactId', inplace=True)
df_pageviews["rating"] = 1

df_pivot_omschrijving = pd.pivot_table(df_omschrijving, index='ContactPersoonId', columns=['Ondernemingsaard', 'Ondernemingstype', 'PrimaireActiviteit', 'Functie'], values='rating', fill_value = 0)

df_pivot_pageviews = pd.pivot_table(df_pageviews, index='ContactId', columns=['PageTitle'], values='rating', fill_value = 0)

df_pivot = pd.concat([df_pivot_omschrijving, df_pivot_pageviews], axis=1, join='inner')


def calc(contact_id: str):
    select_contact = contact_id
    similarity_matrix = cosine_similarity(df_pivot, [df_pivot.loc[select_contact]]).reshape(1,-1)[0]
    similarities = df_pivot.index.join(similarity_matrix)
    similarities = pd.DataFrame({'ContactPersoonId':df_pivot.index, 'sim':similarity_matrix}).set_index('ContactPersoonId')
    similar_users = similarities[similarities['sim'] > 0.5].sort_values(by='sim', ascending=False)
    done_campaigns = df_omschrijving.loc[df_omschrijving.index == select_contact]['CampagneId']
    similar_campagnes = df_omschrijving[df_omschrijving.index.isin(similar_users.index)].replace(0, np.nan).dropna(axis=1, how='all')
    similar_campagnes = similar_campagnes.drop_duplicates(subset=['CampagneId'])
    similar_campagnes = similar_campagnes[similar_campagnes['Startdatum'] > '2023-11-16']
    similar_campagnes_not_done = similar_campagnes[~similar_campagnes['CampagneId'].isin(done_campaigns)][['CampagneId', 'CampagneNaam']]
    return similar_campagnes_not_done


app = FastAPI()


@app.get("/health")
def read_root():
    return {"health": "OK"}

@app.get("/contact/{contact_id}")
def contact_c(contact_id: str):
    return calc(contact_id)