import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sklearn.metrics.pairwise import cosine_similarity
import pickle

from sklearn.decomposition import PCA
from sklearn.preprocessing import LabelEncoder

from fastapi import FastAPI, Query
from typing import Annotated, List

load_dotenv()
DB_URL = os.getenv("DB_URL")
engine = create_engine(DB_URL)
query_main = """
    select c.ContactPersoonId, i.CampagneId, i.CampagneNaam, ca.Startdatum, a.Ondernemingsaard, a.Ondernemingstype, a.PrimaireActiviteit, f.Naam as Functie
    from Contactfiche c
    join Account a on a.AccountId = c.AccountId
    join Inschrijving i on i.ContactficheId = c.ContactPersoonId
    join Campagne ca on ca.CampagneId = i.CampagneId
    join ContactficheFunctie cf on cf.ContactpersoonId = c.ContactPersoonId
    join Functie f on f.FunctieId = cf.FunctieId
    where i.CampagneId is not null;
"""
query_pageviews = """
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

df = pd.read_sql(query_main, engine)
df.set_index('ContactPersoonId', inplace=True)
df["rating"] = 5

df_pageviews = pd.read_sql(query_pageviews, engine)
df_pageviews.set_index('ContactId', inplace=True)
df_pageviews["rating"] = 2
df_pivot_main = pd.pivot_table(df, index='ContactPersoonId', columns=['Ondernemingsaard', 'Ondernemingstype', 'PrimaireActiviteit', 'Functie'], values='rating', fill_value = 0)
df_pivot_main.sort_index(inplace=True)
# df_pivot_pageviews = pd.pivot_table(df_pageviews, index='ContactId', columns=['PageTitle'], values='rating', fill_value = 0)
# df_pivot_pageviews.sort_index(inplace=True)
label_encoder = LabelEncoder()
df_pageviews['CategoryEncoded'] = label_encoder.fit_transform(df_pageviews['PageTitle'])

# # Apply PCA for dimensionality reduction
pca = PCA(n_components=2, random_state=42)
pca_result = pca.fit_transform(df_pageviews[['rating', 'CategoryEncoded']])

# # Create a new DataFrame with the reduced dimensions
pca_df = pd.DataFrame(data=pca_result, columns=['Dimension 1', 'Dimension 2'], index=df_pageviews.index)
grouped_data = pca_df.groupby(pca_df.index).mean()
df_pivot = pd.concat([df_pivot_main, grouped_data], axis=1, join='outer').fillna(0)

script_folder = os.path.dirname(os.path.abspath(__file__))

# model pickle file
model_pickle_file_path = 'LightFM_model.pickle'

# dataset pickle file
dataset_pickle_file_path = 'LightFM_dataset.pickle'

with open(model_pickle_file_path, 'rb') as model_file:
    model = pickle.load(model_file)

with open(dataset_pickle_file_path, 'rb') as dataset_file:
    dataset = pickle.load(dataset_file)
    
def get_top_users_for_item(item_id):
    """
    Returns the top 20 recommended users for a given item, sorted by the number of campaigns entered.

    Parameters:
    item_id (str): The ID of the item.

    Returns:
    str: A string containing the top 20 recommended users for the item, sorted by the number of campaigns entered.
    """
    item_id_internal = dataset.mapping()[2][item_id]
    
    user_ids_internal = np.array(list(dataset.mapping()[0].values()))

    scores = model.predict(user_ids_internal, np.repeat(item_id_internal, len(user_ids_internal)))

    top_users_indices = np.argsort(-scores)[:20]

    top_users_ids = [list(dataset.mapping()[0].keys())[i] for i in top_users_indices]

    return f'Top 20 recommended users for campaign: {top_users_ids}'

def calc(contact_ids: list):
    # select_contact = ['DA252429-E5A6-ED11-AAD1-6045BD8956C9', '915D6FF4-A972-E111-B43A-00505680000A']
    select_contact = contact_ids
    try:
        ss = df_pivot.loc[select_contact]
    except:
        return "id does not exist"
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
    done_campaigns = df.loc[select_contact, ["CampagneId"]]
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
    if not ids:
        return "No ids given"
    ids = ids.split(",")
    return calc(ids)

@app.get("/campagne")
def campagne_api(id: str = Query(None)):
   if not id:
       return "No campaign given"
   return get_top_users_for_item(id)