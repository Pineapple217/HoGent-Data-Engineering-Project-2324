# VOEG DIT TOE AAN FastAPI/app/main.py !!!

import os
import pickle
import numpy as np
from fastapi import FastAPI, Query

script_folder = os.path.dirname(os.path.abspath(__file__))

# model pickle file
model_pickle_file_path = os.path.join(script_folder, 'LightFM_model.pickle')

# dataset pickle file
dataset_pickle_file_path = os.path.join(script_folder, 'LightFM_dataset.pickle')

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

    return f'Top 20 recommended users for item: {top_users_ids}'

# print(get_top_users_for_item('8FCA1D31-1EB7-E811-80F4-001DD8B72B62'))

app = FastAPI()

# ZET UIT COMMENTAAR
@app.get("/campagne")
def campagne_api(id: str = Query(None)):
   if not id:
       return "No campaign given"
   return get_top_users_for_item(id)