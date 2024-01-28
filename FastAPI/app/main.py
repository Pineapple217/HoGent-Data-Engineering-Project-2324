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

df = pd.read_sql(query_main, engine)                # Executes the SQL query 'query_main' and stores the result in DataFrame 'df'
df.set_index('ContactPersoonId', inplace=True)      # Sets 'ContactPersoonId' as the index for 'df', modifying it in place
df["rating"] = 5                                    # Adds a new column named 'rating' to 'df' and assigns a value of 5 to all rows

df_pageviews = pd.read_sql(query_pageviews, engine)  # Executes the SQL query 'query_pageviews' and stores the result in DataFrame 'df_pageviews'
df_pageviews.set_index('ContactId', inplace=True)    # Sets 'ContactId' as the index for 'df_pageviews', modifying it in place
df_pageviews["rating"] = 2                           # Adds a new column named 'rating' to 'df_pageviews' and assigns a value of 2 to all rows

# Creates a pivot table from 'df', indexed by 'ContactPersoonId', with multi-level columns from specified features, and 'rating' as values
df_pivot_main = pd.pivot_table(df, index='ContactPersoonId', columns=['Ondernemingsaard', 'Ondernemingstype', 'PrimaireActiviteit', 'Functie'], values='rating', fill_value=0)
df_pivot_main.sort_index(inplace=True)              # Sorts the 'df_pivot_main' DataFrame by index in place

# The following two lines are commented out and thus not executed
# df_pivot_pageviews = pd.pivot_table(df_pageviews, index='ContactId', columns=['PageTitle'], values='rating', fill_value=0)
# df_pivot_pageviews.sort_index(inplace=True)

# Instantiates a LabelEncoder to encode categorical labels with numerical values
label_encoder = LabelEncoder()
df_pageviews['CategoryEncoded'] = label_encoder.fit_transform(df_pageviews['PageTitle']) # Applies LabelEncoder to the 'PageTitle' column in 'df_pageviews' and stores the result in a new column 'CategoryEncoded'

# Instantiates PCA for dimensionality reduction, setting the number of components to 2 and a random state for reproducibility
pca = PCA(n_components=2, random_state=42)
pca_result = pca.fit_transform(df_pageviews[['rating', 'CategoryEncoded']])              # Applies PCA to selected columns in 'df_pageviews' and stores the result in 'pca_result'

# Creates a new DataFrame 'pca_df' from 'pca_result', setting column names and using the index from 'df_pageviews'
pca_df = pd.DataFrame(data=pca_result, columns=['Dimension 1', 'Dimension 2'], index=df_pageviews.index)

grouped_data = pca_df.groupby(pca_df.index).mean()                                      # Groups 'pca_df' by index and calculates the mean for each group
df_pivot = pd.concat([df_pivot_main, grouped_data], axis=1, join='outer').fillna(0)     # Concatenates 'df_pivot_main' and 'grouped_data' into a new DataFrame 'df_pivot', handling missing values with fillna(0)


script_folder = os.path.dirname(os.path.abspath(__file__)) # Determines the folder where the current script is located

model_pickle_file_path = 'LightFM_model.pickle'     # Sets the path for the LightFM model pickle file

dataset_pickle_file_path = 'LightFM_dataset.pickle' # Sets the path for the LightFM dataset pickle file


    
# Functie voor epic 5
def get_top_users_for_item(item_id):
    """
    Returns the top 20 recommended users for a given item, sorted by the number of campaigns entered.

    Parameters:
    item_id (str): The ID of the item.

    Returns:
    str: A string containing the top 20 recommended users for the item, sorted by the number of campaigns entered.
    """
    with open(model_pickle_file_path, 'rb') as model_file:
        model = pickle.load(model_file)                 # Loads the LightFM model from the pickle file

    with open(dataset_pickle_file_path, 'rb') as dataset_file:
        dataset = pickle.load(dataset_file)             # Loads the dataset from the pickle file
    item_id_internal = dataset.mapping()[2][item_id]    # Maps the external item ID to the internal ID used in the dataset
    
    user_ids_internal = np.array(list(dataset.mapping()[0].values()))  # Retrieves internal user IDs from the dataset

    # Predicts scores for all users for the given item
    scores = model.predict(user_ids_internal, np.repeat(item_id_internal, len(user_ids_internal)))

    top_users_indices = np.argsort(-scores)[:20]  # Retrieves indices of the top 20 users based on the scores

    top_users_ids = [list(dataset.mapping()[0].keys())[i] for i in top_users_indices]  # Maps indices back to user IDs

    x = {"Top 20 recommended users for campaign": top_users_ids}  # Prepares the result in a dictionary format

    return x  # Returns the result

# Functie voor epic 3
def calc(contact_ids: list):
    # select_contact = ['DA252429-E5A6-ED11-AAD1-6045BD8956C9', '915D6FF4-A972-E111-B43A-00505680000A']
    select_contact = contact_ids            # Assigns the input contact IDs to 'select_contact'
    try:
        ss = df_pivot.loc[select_contact]   # Tries to locate the rows in 'df_pivot' corresponding to the contact IDs
    except:
        return "id does not exist"          # Returns an error message if IDs are not found
    try: 
        similarity_matrix = cosine_similarity(df_pivot, ss)  # Computes cosine similarity between 'df_pivot' and 'ss'
    except:
        return "geen sims"                  # Returns an error message if similarity computation fails
    
    similarity_matrix_df = pd.DataFrame(similarity_matrix, index=df_pivot.index, columns=ss.index)  # Creates a DataFrame from the similarity matrix
    stacked_similarity_df = similarity_matrix_df.stack()  # Stacks the DataFrame to a Series

    high_similarity_df = pd.DataFrame(stacked_similarity_df[stacked_similarity_df > 0.5])  # Filters out similarities greater than 0.5
    high_similarity_df.index.names = ['Similars', 'Selected']               # Names the indices of the DataFrame
    high_similarity_df.reset_index(inplace=True)                            # Resets the index of the DataFrame
    high_similarity_df.set_index(['Selected', 'Similars'], inplace=True)    # Sets a multi-level index for the DataFrame
    high_similarity_df.sort_index(level='Selected', inplace=True)           # Sorts the DataFrame by the 'Selected' level of the index

    done_campaigns = df.loc[select_contact, ["CampagneId"]]  # Retrieves the campaign IDs for the selected contacts
    results = []  # empty list for results
    for selected, s_df in high_similarity_df.groupby(level=0):      # Iterates over groups in 'high_similarity_df' grouped by 'Selected'
        t = df[df.index.isin(s_df.reset_index()['Similars'])].replace(0, np.nan).dropna(axis=1, how='all')  # Filters and cleans the DataFrame
        t.sort_values('Startdatum', ascending=False, inplace=True)  # Sorts filtered df by 'Startdatum' in descending order
        t = t[t['Startdatum'] > '2023-11-06']                       # Filters sorted df for start dates after a specific date
        results.append((selected, t))                               # Appends the result to the results list
    r = []  # empty list for formatted results

    for s, x in results:  # Iterates over the results
        similar_campagnes_not_done = x[~x['CampagneId'].isin(done_campaigns.loc[s])][['CampagneId', 'CampagneNaam']]    # Filters campaigns not done
        similar_campagnes_not_done.drop_duplicates(inplace=True)                                                        # Removes duplicates
        similar_campagnes_not_done.set_index('CampagneId')                                                              # Sets 'CampagneId' as the index
        x = similar_campagnes_not_done.reset_index()[['CampagneId', 'CampagneNaam']].set_index('CampagneId').to_dict()  # Formats the result
        x['contact'] = s    # Adds the contact ID to the result
        r.append(x)         # Appends the formatted result to 'r'
    return r               # Returns the final results

app = FastAPI()  # Initializes a FastAPI application



# Decorator to create a new route with the path "/health" that handles GET requests
@app.get("/health")
def read_root():
    return {"health": "OK"}  # Returns a JSON response indicating the health status of the API

# Decorator to create a new route with the path "/contact" that handles GET requests
@app.get("/contact")
def contact_c(ids: str = Query(None)):  # Defines a function to handle requests to "/contact", with an optional query parameter 'ids'
    if not ids:
        return "No ids given"   # Returns an error message if no 'ids' query parameter is provided
    ids = ids.split(",")        # Splits the 'ids' string into a list of IDs
    return calc(ids)            # Calls the 'calc' function with the list of IDs and returns its result

# Decorator to create a new route with the path "/campagne" that handles GET requests
@app.get("/campagne")
def campagne_api(id: str = Query(None)):# Defines a function to handle requests to "/campagne", with an optional query parameter 'id'
   if not id:
       return "No campaign given"       # Returns an error message if no 'id' query parameter is provided
   return get_top_users_for_item(id)    # Calls the 'get_top_users_for_item' function with the 'id' and returns its result
