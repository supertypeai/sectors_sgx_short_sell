import pandas as pd
from supabase import create_client
import os
from dotenv import load_dotenv
from thefuzz import fuzz
import logging
logging.basicConfig(level=logging.ERROR)

load_dotenv()

def preprocess_names(name_list: list) -> list:
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
  return cleaned_unique_value_short_sell

if __name__ == "__main__":

  url_supabase = os.getenv("SUPABASE_URL")
  key = os.getenv("SUPABASE_KEY")
  supabase = create_client(url_supabase, key)

  # Get the table
  limit = 3000
  offset = 0
  all_data = []

  while True:
    response = supabase.table("sgx_short_sell").select("").range(offset, offset + limit - 1).execute()
    data = response.data
    all_data.extend(data)

    if len(data) < limit:
        break

    offset += limit
    print(f"Getting Data... Offset {offset} - {offset+limit-1}")

  df_short_sell = pd.DataFrame(all_data)
  df_short_sell = df_short_sell.sort_values(['name'], ascending=True)
  df_short_sell = df_short_sell.replace("", None)


  # Get the table
  db_data = supabase.table("sgx_companies").select("").execute()
  df_companies = pd.DataFrame(db_data.data)

  # Get only the name and the symbol
  cols = df_companies.columns.tolist()
  cols.remove("name")
  cols.remove("symbol")
  df_companies_clean = df_companies.drop(cols, axis=1)
  df_companies_clean.head()

  companies_dict = df_companies_clean.to_dict(orient="records")
  
  unique_value_short_sell = df_short_sell['name'].unique()
  cleaned_unique_value_short_sell = preprocess_names(unique_value_short_sell)

