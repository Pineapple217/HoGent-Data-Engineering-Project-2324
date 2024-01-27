## 1. Imports
import os
import pickle
import sqlalchemy
import numpy as np
import pandas as pd
from tqdm import tqdm
from dotenv import load_dotenv
from lightfm.data import Dataset
from lightfm.evaluation import auc_score
from sqlalchemy import create_engine, text
from lightfm import LightFM, cross_validation


## 2. Variabelen
SEED = 42
NO_THREADS = 8
NO_EPOCHS = 15
NO_COMPONENTS = 20
TEST_PERCENTAGE = 0.1
LEARNING_RATE = 0.2
ITEM_ALPHA = 1e-7
USER_ALPHA = 1e-7
LOSS = 'logistic'
CHECKPOINT = 'LightFM'


## 3. Data inladen
load_dotenv()
DB_URL = os.getenv("DB_URL")
engine = create_engine(DB_URL)

try:
    connection = engine.connect()
    print("Successfully connected to the database")
except Exception as e:
    print(f"Failed to connect to the database: {e}")

print(f"SQLAlchemy version: {sqlalchemy.__version__}")

query = text('SELECT * FROM epic_5')

try:
    df = pd.read_sql_query(query, connection)
    print("Successfully executed query")
except Exception as e:
    print(f"Failed to execute query: {e}")


## 4. Data voorbereiden
columns = ['ThemaDuurzaamheid', 'ThemaFinancieelFiscaal', 'ThemaInnovatie', 'ThemaInternationaalOndernemen', 
           'ThemaMobiliteit', 'ThemaOmgeving', 'ThemaSalesMarketingCommunicatie', 
           'ThemaStrategieEnAlgemeenManagement', 'ThemaTalent', 'ThemaWelzijn']

for col in columns:
    df[col] = df[col].replace({0: col + '_False', 1: col + '_True'})

df.drop('aantal_bezoeken', axis=1, inplace=True)

item_cols = ['SessieThema', 'SoortCampagne', 'TypeCampagne']
user_cols = ['ThemaDuurzaamheid', 'ThemaFinancieelFiscaal', 'ThemaInnovatie', 'ThemaInternationaalOndernemen', 'ThemaMobiliteit', 'ThemaOmgeving', 'ThemaSalesMarketingCommunicatie', 'ThemaStrategieEnAlgemeenManagement', 'ThemaTalent', 'ThemaWelzijn']

all_item_features = np.concatenate([df[col].unique() for col in item_cols]).tolist()
all_user_features = np.concatenate([df[col].unique() for col in user_cols]).tolist()

items = df[['CampagneId'] + item_cols]
users = df[['PersoonId'] + user_cols]

df = df.groupby(['PersoonId', 'CampagneId'])['aantal_sessies'].sum().reset_index()

dataset = Dataset()
dataset.fit(
    users=df['PersoonId'],
    items=df['CampagneId'],
    user_features=all_user_features,
    item_features=all_item_features
)

num_users, num_items = dataset.interactions_shape()
(interactions, weights) = dataset.build_interactions(zip(df['PersoonId'], df['CampagneId'], df['aantal_sessies']))

train_interactions, test_interactions = cross_validation.random_train_test_split(
    interactions, test_percentage=TEST_PERCENTAGE,
    random_state=np.random.RandomState(seed=SEED)
)

def item_feature_generator():
    for _, row in items.iterrows():
        features = row.values[1:]
        yield (row['CampagneId'], features)

def user_feature_generator():
    for _, row in users.iterrows():
        features = row.values[1:]
        yield (row['PersoonId'], features)

item_features = dataset.build_item_features((item_id, item_feature) for item_id, item_feature in item_feature_generator())
user_features = dataset.build_user_features((user_id, user_feature) for user_id, user_feature in user_feature_generator())


## 5. Model trainen

print('Training LightFM model...')

model = LightFM(
    no_components=NO_COMPONENTS,
    learning_rate=LEARNING_RATE,
    random_state=np.random.RandomState(SEED),
    loss=LOSS,
    item_alpha=ITEM_ALPHA,
    user_alpha=USER_ALPHA
)


def save_model(model):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    with open(os.path.join('/app', f'{CHECKPOINT}_model.pickle'), 'wb') as fle:
        pickle.dump(model, fle, protocol=pickle.HIGHEST_PROTOCOL)

def save_dataset(dataset):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    with open(os.path.join('/app', f'{CHECKPOINT}_dataset.pickle'), 'wb') as fle:
        pickle.dump(dataset, fle, protocol=pickle.HIGHEST_PROTOCOL)
        
save_dataset(dataset)

train_auc_history = []
test_auc_history = []

best_score = 0

for epoch in tqdm(range(NO_EPOCHS)):
    model.fit_partial(
        interactions=train_interactions,
        user_features=user_features,
        item_features=item_features,
        epochs=NO_EPOCHS,
        num_threads=NO_THREADS
    )

    train_auc = auc_score(model, train_interactions, user_features=user_features, item_features=item_features).mean()
    test_auc = auc_score(model, test_interactions, train_interactions, user_features=user_features, item_features=item_features).mean()

    train_auc_history.append(train_auc)
    test_auc_history.append(test_auc)

    if test_auc > best_score:
        best_score = test_auc
        save_model(model)

    print(f'Epoch {epoch + 1}/{NO_EPOCHS}, Train AUC: {train_auc}, Test AUC: {test_auc}')

print('Training complete')