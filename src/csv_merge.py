import logging
import pandas as pd
from pathlib import Path
import re
import os
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
load_dotenv()
MERGE_PATH = os.getenv("MERGE_PATH")


# def main():
#     df1_folder = MERGED_PATH1
#     df2_folder = MERGED_PATH2
#     folder_name = '../DataMerged'
#     if not os.path.exists(folder_name):
#         os.makedirs(folder_name)
#     for filename in os.listdir(df1_folder):
#         if filename.endswith('.csv'):
#             df1_file = os.path.join(df1_folder, filename)
#             df2_file = os.path.join(df2_folder, filename)
            
#             logger.info(df1_file)
#             if not Path(df2_file).is_file():
#                 logger.warning(f"{df2_file} not found")

#                 continue
#             c1 = pd.read_csv(df1_file, delimiter=",", encoding="utf-8", keep_default_na=True, na_values=[""], chunksize=10_000)
#             df1 = pd.concat(c1)
#             c2 = pd.read_csv(df2_file, delimiter=",", encoding="utf-8", keep_default_na=True, na_values=[""], chunksize=10_000)
#             df2 = pd.concat(c2)
#             merged_df = pd.concat([df1, df2], ignore_index=True).sort_values(df1.columns[0]).drop_duplicates(df1.columns[0], keep='last')
#             file_path = os.path.join(folder_name, filename)
#             merged_df.to_csv(file_path, index=False)


def merge_dataframes(df_folders, output_folder):
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    for folder in df_folders:
        folder = os.path.join(MERGE_PATH, folder)
        for filename in os.listdir(folder):
            if filename.endswith('.csv'):
                df_file = os.path.join(folder, filename)
                
                logger.info(df_file)
                # if not Path(df_file).is_file():
                #     logger.warning(f"{df_file} not found")
                #     continue

                chunks = pd.read_csv(df_file, delimiter=",", encoding="utf-8-sig", keep_default_na=True, na_values=[""], chunksize=1_000, on_bad_lines='skip')
                df = pd.concat(chunks)
                file_path = os.path.join(output_folder, filename)

                if os.path.exists(file_path):
                    existing_df = pd.read_csv(file_path)
                    merged_df = pd.concat([existing_df, df], ignore_index=True)
                    # .sort_values(existing_df.columns[0]).drop_duplicates(existing_df.columns[0], keep='last')
                else:
                    merged_df = df

                merged_df.to_csv(file_path, index=False)

def main():
    df_folders = os.listdir(MERGE_PATH)
    print(df_folders)
    output_folder = '../DataMerged'
    merge_dataframes(df_folders, output_folder)