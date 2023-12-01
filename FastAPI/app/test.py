import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sklearn.metrics.pairwise import cosine_similarity

from fastapi import FastAPI, Query
from typing import Annotated, List

load_dotenv()
DB_URL = os.getenv("DB_URL")
engine = create_engine(DB_URL)
query = """
    select c.ContactPersoonId, i.CampagneId, i.CampagneNaam, ca.Startdatum, a.Ondernemingsaard, a.Ondernemingstype, a.PrimaireActiviteit, f.Naam as Functie
    from Contactfiche c
    join Account a on a.AccountId = c.AccountId
    join Inschrijving i on i.ContactficheId = c.ContactPersoonId
    join Campagne ca on ca.CampagneId = i.CampagneId
    join ContactficheFunctie cf on cf.ContactpersoonId = c.ContactPersoonId
    join Functie f on f.FunctieId = cf.FunctieId
    where i.CampagneId is not null;
"""
df= pd.read_sql(query, engine)
df.set_index('ContactPersoonId', inplace=True)
df["rating"] = 5

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
df_pageviews = pd.read_sql(query2, engine)
df_pageviews.set_index('ContactId', inplace=True)
df_pageviews["rating"] = 2

df_campagne = df.iloc[:, 0:3]
df_campagne = df_campagne[df_campagne['Startdatum'] > '2022-11-06']
df_campagne['rating'] = 10
df_omschrijving = df.iloc[:, 3:]

def create_set_of_index(dfs: list):
    """
    Creates a set of all the indexes of the dataframes in the list
    :param dfs: list of dataframes
    :return: set of indexes
    """
    set_of_index = set()
    for df in dfs:
        set_of_index = set_of_index.union(set(df.index))
    return set_of_index

def add_indexes(df: pd.DataFrame, indexes: set):
    """
    Adds the missing indexes to the dataframe
    :param df: dataframe
    :param indexes: set of indexes
    :return: dataframe with added indexes
    """
    remaining_indexes = indexes.difference(set(df.index))
    df2 = pd.DataFrame(index=list(remaining_indexes), columns=df.columns)
    df = df._append(df2)
    df.index.name = 'ContactPersoonId'
    return df.fillna(0).astype('int8')

df_pivot_omschrijving = pd.pivot_table(df_omschrijving, index='ContactPersoonId', columns=['Ondernemingsaard', 'Ondernemingstype', 'PrimaireActiviteit', 'Functie'], values='rating', fill_value = 0).fillna(0).astype('int8')

df_pivot_campagne = pd.pivot_table(df_campagne, index='ContactPersoonId', columns=['CampagneId'], values='rating', fill_value = 0).fillna(0).astype('int8')

df_pivot_pageviews = pd.pivot_table(df_pageviews, index='ContactId', columns=['PageTitle'], values='rating', fill_value = 0).fillna(0).astype('int8')

indexes = create_set_of_index([df_pivot_omschrijving, df_pivot_campagne, df_pivot_pageviews])

df_pivot_omschrijving = add_indexes(df_pivot_omschrijving, indexes)
df_pivot_campagne = add_indexes(df_pivot_campagne, indexes)
df_pivot_pageviews = add_indexes(df_pivot_pageviews, indexes)

df_pivot = pd.concat([df_pivot_omschrijving, df_pivot_pageviews, df_pivot_campagne], axis=1, join='outer')

def calc(contact_ids: list):
    # select_contact = ['DA252429-E5A6-ED11-AAD1-6045BD8956C9', '915D6FF4-A972-E111-B43A-00505680000A']
    select_contact = contact_ids
    try:
        ss = df_pivot.loc[select_contact]
    except:
        return "id does not exits"
    try: 
        similarity_matrix = cosine_similarity(df_pivot, ss)
    except:
        return "geen sims"
    
    similarity_matrix_df = pd.DataFrame(similarity_matrix, index=df_pivot.index, columns=ss.index)
    stacked_similarity_df = similarity_matrix_df.stack()
    high_similarity_df = pd.DataFrame(stacked_similarity_df[stacked_similarity_df > 0.5])
    high_similarity_df.index.names = ['Similars', 'Selected']
    high_similarity_df.reset_index(inplace=True)
    high_similarity_df.set_index(['Selected', 'Similars'], inplace=True)
    high_similarity_df.sort_index(level='Selected', inplace=True)
    done_campaigns = df.loc[select_contact]["CampagneId"].to_frame()
    results = []
    for selected, s_df in high_similarity_df.groupby(level=0):
        t = df[df.index.isin(s_df.reset_index()['Similars'])].replace(0, np.nan).dropna(axis=1, how='all')
        t.sort_values('Startdatum', ascending=False, inplace=True)
        t = t[t['Startdatum'] > '2023-11-06']
        results.append((selected, t))
    r = []
    for s, x in results:
        similar_campagnes_not_done = x[~x['CampagneId'].isin(done_campaigns.loc[s])][['CampagneId', 'CampagneNaam']]
        similar_campagnes_not_done.drop_duplicates(inplace=True)
        similar_campagnes_not_done.set_index('CampagneId')
        x = similar_campagnes_not_done.reset_index()[['CampagneId', 'CampagneNaam']].set_index('CampagneId').to_dict()
        x['contact'] = s
        r.append(x)
    return r

app = FastAPI()


@app.get("/health")
def read_root():
    return {"health": "OK"}

@app.get("/contact")
def contact_c(ids: str = Query(None)):
    ids = ids.split(",")
    return calc(ids)
