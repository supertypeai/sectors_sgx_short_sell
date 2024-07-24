import pandas as pd
from supabase import create_client
import os
import json
from dotenv import load_dotenv
from thefuzz import fuzz
import logging
logging.basicConfig(level=logging.ERROR)

load_dotenv()


# CONSTANT
THRESHOLD = 95


def preprocess_names(unique_value_short_sell: list) -> list:
  cleaned_unique_value_short_sell = list()

  # DATA CLEANING
  for name in unique_value_short_sell:
    # Common words such as 'ltd', 'sgd', 'reit, 'intl', etc should be adjusted
    # Use space in front of the words to make sure to delete the 'distinct' words
    common_words = {
      ' sgd': '',
      ' sg': '', 
      ' intl': ' International', 
      ' gbl': ' Global', 
      # Cannot delete foreign currencies
      # 'usd': '',
      # 'gbp': '', 
      # 'hk': '', 
      # 'hkd': '', 
      ' grp': ' Group', 
      ' htrust': ' Hospitality Trust',
      ' intcom' : ' Integrated Commercial',
      ' tv': ' Television',
      ' tr': ' Trust',
      ' ind ': ' Industrial ',
      ' log': ' Logistics',
      ' com': ' Commercial',
      ' inv ': ' Investment ',
      ' hldg': ' Holding', 
      ' fin': ' Finance',
      ' shipbldg': ' Shipbuilding',
    }

    for key, val in common_words.items():
      name = name.replace(key, val)

    # Handling for stock special cases
    # For future: Need to be adjusted manually if needed
    special_cases = {
      "beverlyjcg" : "Beverly JCG",
      "capitalandinvest" : "Capitaland Investment",
      "capland" : "Capitaland",
      "chinakundatech" : "China Kunda Tech",
      "chinasunsine" : "China Sunsine",
      "citydev" : "City Development",
      "cosco Shipbuilding" : "cosco",
      "daiwa hse" : "Daiwa House",
      "digicore" : "Digital Core",
      "frasers cpt" : "Frasers Centrepoint",
      "fsl" : "First Ship Lease",
      "g invacom" : "Global Invacom",
      "golden agri-res" : "GoldenAgr",
      "hongkongland" : "HK Land",
      "hph" : "Hutchison Port Holdings",
      "hpl" : "Hotel Properties Ltd",
      "jmh" : "Jardine Matheson Holdings",
      "kep infra": "Keppel Infra REIT",
      "keppacoak": "Keppel Pacific Oak",
      "marcopolo" : "Marco Polo",
      "manulifereit usd" : "Manulife US RE",
      "namcheong" : "Nam Cheong",
      "ouereit" : "OUE REIT",
      "pacificradiance" : "Pacific Radiance",
      "panunited" : "Pan United",
      "parkwaylife" : "Parkway Life",
      "resourcesgbl" : "Resources Global",
      "samuderashipping" : "Samudera Shipping",
      "seatrium ltd" : "Seatrium Limited",
      "sembcorp" : "Semb Corp",
      "sgx" : "Singapore Exchange",
      "singholdings" : "Sing Holdings",
      "singpost" : "Singapore Post",
      "singshipping" : "Singapore Shipping",
      "sin heng mach" : "Sin Heng Heavy Machinery",
      "southernalliance" : "Southern Alliance",
      "starhillgbl" : "Starhill Global",
      "sunmoonfood" : "SunMoon Food",
      "tat seng pkg" : "Tat Seng Packaging",
      "thaibev" : "Thai Beverage",
      "tj darentang" : "Tianjin Zhongxin Pharma Group",
      "ughealthcare" : "UG Healthcare",
      "uoi" : "United Overseas Insurance",
      "utdhampsh" : "United Hampshire",
      "winkingstudios" : "Wingking Studios",
      "yzj Finance" : "Yangzijiang Financial",
      "yzj" : "Yangzijiang",
    }

    for key, val in special_cases.items():
      name = name.replace(key, val)
    
    cleaned_unique_value_short_sell.append(name)

  # Preprocess is done
  print("[PROGRESS] Preprocessing is done...")
  return cleaned_unique_value_short_sell


def match_names(cleaned_unique_value_short_sell : list, unique_value_short_sell: list, companies_dict_list: dict) -> list:
  list_of_dictionaries = list()
  for i in range (len(cleaned_unique_value_short_sell)):
    original_name = unique_value_short_sell[i]
    name = cleaned_unique_value_short_sell[i]
    name_dict = {
      "name" : original_name,
      "cleaned_name" : name,
      "partial_ratio" : list(),
      "token_set_ratio" : list(),
      "token_sort_ratio" : list(),
      "partial_token_sort_ratio" : list(),
    }
    
    # Reset variable value
    max_partial_rat = 0
    max_token_sort_rat = 0
    max_token_set_rat = 0
    max_partial_token_sort_ratio = 0

    for company_data in companies_dict_list:
      c_name = company_data['name']
      c_symbol = company_data['symbol']

      partial_rat = 0
      token_sort_rat = 0
      token_set_rat = 0
      partial_token_sort_ratio = 0

      # Make both upper
      name_upper = name.upper()
      c_name_upper = c_name.upper()

      partial_rat = fuzz.partial_ratio(name_upper, c_name_upper)
      token_sort_rat = fuzz.token_sort_ratio(name_upper, c_name_upper)
      token_set_rat = fuzz.token_set_ratio(name_upper, c_name_upper)
      partial_token_sort_ratio = fuzz.partial_token_sort_ratio(name_upper, c_name_upper)

      if (partial_rat >= THRESHOLD and partial_rat > max_partial_rat):
        temp_dict = {
          "name" : c_name,
          "symbol" : c_symbol,
          "value" : partial_rat
        }
        name_dict['partial_ratio'].append(temp_dict)
        max_partial_rat = partial_rat
      
      if (token_sort_rat >= THRESHOLD and token_sort_rat > max_token_sort_rat):
        temp_dict = {
          "name" : c_name,
          "symbol" : c_symbol,
          "value" : token_sort_rat
        }
        name_dict['token_sort_ratio'].append(temp_dict)
        max_token_sort_rat = token_sort_rat
      
      if (token_set_rat >= THRESHOLD and token_set_rat > max_token_set_rat):
        temp_dict = {
          "name" : c_name,
          "symbol" : c_symbol,
          "value" : token_set_rat
        }
        name_dict['token_set_ratio'].append(temp_dict)
        max_token_set_rat = token_set_rat
      
      if (partial_token_sort_ratio >= THRESHOLD and partial_token_sort_ratio > max_partial_token_sort_ratio):
        temp_dict = {
          "name" : c_name,
          "symbol" : c_symbol,
          "value" : partial_token_sort_ratio
        }
        name_dict['partial_token_sort_ratio'].append(temp_dict)
        max_partial_token_sort_ratio = partial_token_sort_ratio
      
      
    list_of_dictionaries.append(name_dict)
  
  # Matching process is done
  print("[PROGRESS] String matching is done...")
  return list_of_dictionaries

def vote_names(list_of_dictionaries: list) -> tuple:
  still_null_data = list() # List of name
  final_data = dict() # List of dict

  for dictionary in list_of_dictionaries:
    partial_ratio_data = dictionary['partial_ratio']
    token_set_ratio_data = dictionary['token_set_ratio']
    token_sort_ratio_data = dictionary['token_sort_ratio']
    partial_token_sort_ratio_data = dictionary['partial_token_sort_ratio']

    sum_len_data = len(partial_ratio_data) + len(token_set_ratio_data) + len(token_sort_ratio_data) + len(partial_token_sort_ratio_data)
    
    if ( sum_len_data == 0):
      still_null_data.append(dictionary['name'])
      # print(f"[NONE] {dictionary['name']}")
    else:
      # Process to vote
      value_dict = dict()
      
      if len(partial_ratio_data) > 0:
        for data in partial_ratio_data:
          if (data['symbol'] not in value_dict):
            value_dict[data['symbol']] = {
              "name" : data['name'],
              "value" : data['value']
            }
          else:
            value_dict[data['symbol']]['value'] += data['value']
      
      if len(token_set_ratio_data) > 0:
        for data in token_set_ratio_data:
          if (data['symbol'] not in value_dict):
            value_dict[data['symbol']] = {
              "name" : data['name'],
              "value" : data['value']
            }
          else:
            value_dict[data['symbol']]['value'] += data['value']
      
      if len(token_sort_ratio_data) > 0:
        for data in token_sort_ratio_data:
          if (data['symbol'] not in value_dict):
            value_dict[data['symbol']] = {
              "name" : data['name'],
              "value" : data['value']
            }
          else:
            value_dict[data['symbol']]['value'] += data['value']
      
      if len(partial_token_sort_ratio_data) > 0:
        for data in partial_token_sort_ratio_data:
          if (data['symbol'] not in value_dict):
            value_dict[data['symbol']] = {
              "name" : data['name'],
              "value" : data['value']
            }
          else:
            value_dict[data['symbol']]['value'] += data['value']
    

      # Get the max from value_dict based on 'value
      key_max = max(value_dict, key = lambda x: value_dict[x]['value'])  

      final_data[dictionary['name']] = {
          "symbol" : key_max,
          "name" : value_dict[key_max]['name'],
          "value" : value_dict[key_max]['value']
        }
  
  # Voting process is done
  print("[PROGRESS] Voting process is done...")
  return final_data, still_null_data

def save_names(final_data: dict, still_null_data: list):
  cwd = os.getcwd()
  data_dir = os.path.join(cwd, "data")
  filename = os.path.join(data_dir, f"result_{THRESHOLD}.json")
  not_found_filename = os.path.join(data_dir, f"not_found_{THRESHOLD}.json")

  with open(filename, "w") as outfile: 
      json.dump(final_data, outfile, indent=2)
  with open(not_found_filename, "w") as outfile: 
      json.dump(still_null_data, outfile, indent=2)
  print(f"[PROGRESS] Files are saved in {filename} and {not_found_filename}")

def insert_names_to_df(final_data: dict, df_short_sell: pd.DataFrame) -> pd.DataFrame:
  for index, row in df_short_sell.iterrows():
    if (row['name'] in final_data):
      new_symbol = final_data[row['name']]['symbol']
      df_short_sell.at[index, "symbol"] = new_symbol

  df_short_sell_filled = df_short_sell[df_short_sell['symbol'].notnull()]
  print("[PROGRESS] Inserting to Dataframe process is done...")
  return df_short_sell_filled


# Only run to test the code
# if __name__ == "__main__":
#   url_supabase = os.getenv("SUPABASE_URL")
#   key = os.getenv("SUPABASE_KEY")
#   supabase = create_client(url_supabase, key)

#   # Get the table
#   limit = 3000
#   offset = 0
#   all_data = []

#   while True:
#     response = supabase.table("sgx_short_sell").select("").range(offset, offset + limit - 1).execute()
#     data = response.data
#     all_data.extend(data)

#     if len(data) < limit:
#         break

#     offset += limit
#     print(f"Getting Data... Offset {offset} - {offset+limit-1}")

#   df_short_sell = pd.DataFrame(all_data)
#   df_short_sell = df_short_sell.sort_values(['name'], ascending=True)
#   df_short_sell = df_short_sell.replace("", None)


#   # Get the table
#   db_data = supabase.table("sgx_companies").select("").execute()
#   df_companies = pd.DataFrame(db_data.data)

#   # Get only the name and the symbol
#   cols = df_companies.columns.tolist()
#   cols.remove("name")
#   cols.remove("symbol")
#   df_companies_clean = df_companies.drop(cols, axis=1)

#   companies_dict_list = df_companies_clean.to_dict(orient="records")
  
#   unique_value_short_sell = df_short_sell['name'].unique()
#   cleaned_unique_value_short_sell = preprocess_names(unique_value_short_sell)
#   list_of_dictionaries = match_names(cleaned_unique_value_short_sell, unique_value_short_sell, companies_dict_list)
#   final_data, still_null_data = vote_names(list_of_dictionaries)

#   # save_names(final_data, still_null_data) # Only if needed
#   df_short_sell_filled = insert_names_to_df(final_data, df_short_sell)

#   # Convert to json. Remove the index in dataframe
#   records = df_short_sell_filled.to_dict(orient="records")

#   # Upsert to db
#   try:
#     supabase.table("sgx_short_sell").upsert(
#           records
#       ).execute()
#     print(
#         f"Successfully upserted {len(records)} data to database"
#     )
#   except Exception as e:
#     raise Exception(f"Error upserting to database: {e}")


