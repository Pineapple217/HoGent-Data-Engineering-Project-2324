import pandas as pd
import datetime
import os

def load_csv(file_path):
    #hardcode delimiters en encodings
    delimiters = [',', ';']  
    encodings = ['utf-8', 'latin1', 'utf-8-sig']  

    for encoding in encodings:
        for delimiter in delimiters:
            try:
                return pd.read_csv(file_path, encoding=encoding, delimiter=delimiter, low_memory=False, keep_default_na=True, na_values=[""]), None
            except (UnicodeDecodeError, pd.errors.ParserError):
                continue  #probeer volgende combinatie

    return None, "Unable to read file with encodings 'utf-8', 'latin1' and 'utf-8-sig' and delimiters ',' and ';' "

# verplaatst het csv bestand naar de "old" map met een timestamp om naamconflicten te voorkomen en data bij te houden
def move_csv_file(csv_path, destination_folder):
    base_name = os.path.basename(csv_path)
    file_name, file_extension = os.path.splitext(base_name)

    timestamp_str = datetime.datetime.now().strftime("%Y_%m_%d_%H-%M-%S")
    new_path = os.path.join(destination_folder, f"{file_name}_{timestamp_str}{file_extension}")
    
    os.rename(csv_path, new_path)

#code misschien voor later
# def vergelijkHeaders(oldfile, newfile, path_old, path_new):
#     try:
#         # laad data via bovenstaande functie
#         df1, error1 = load_csv(os.path.join(path_old, oldfile))
#         df2, error2 = load_csv(os.path.join(path_new, newfile))

#         # Check for loading errors
#         if error1 or error2:
#             return f"Error loading files: {error1 or ''} {error2 or ''}", None

#         # Create lists of headers
#         headers1 = list(df1.columns)
#         headers2 = list(df2.columns)

#         # Check if all headers are the same
#         if set(headers1) == set(headers2):
#             return True, None 
#         else:
#             # Create a dictionary for columns in DataFrame
#             data = {'Columns in ' + oldfile: [], 'Columns in ' + newfile: []}

#             # Add matching headers
#             for col in headers1:
#                 if col in headers2:
#                     data['Columns in ' + oldfile].append(col)
#                     data['Columns in ' + newfile].append(col)

#             # Add unique headers, with None in the other column
#             for col in headers1:
#                 if col not in headers2:
#                     data['Columns in ' + oldfile].append(col)
#                     data['Columns in ' + newfile].append(None)

#             for col in headers2:
#                 if col not in headers1:
#                     data['Columns in ' + oldfile].append(None)
#                     data['Columns in ' + newfile].append(col)

#             # Create a DataFrame for comparison
#             comparison_df = pd.DataFrame(data)

#             return False, comparison_df
#     except Exception as e:
#         return f"An error occurred: {e}", None
    
# def checkHeaders(df, path_old, path_new):
#     for row in range(len(df)):
#         result, comparison_df = vergelijkHeaders(df.iloc[row,0], df.iloc[row,1], path_old, path_new)

#         if not result:
#             print(df.iloc[row,0], df.iloc[row,1])
#             print("Headers are not equal. Comparison:")
#             print(comparison_df)