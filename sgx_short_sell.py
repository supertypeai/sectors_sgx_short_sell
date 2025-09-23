import requests
import pandas as pd
import numpy as np
import re
from supabase import create_client
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import logging
from imp import reload
logging.basicConfig(level=logging.ERROR)
import argparse
import sys
from function_thefuzz import preprocess_names, match_names, vote_names, save_names, insert_names_to_df

load_dotenv()

def extract_txt(text_data):
    
    # Split the content into lines
    lines = text_data.split('\r\n')

    # Extract the header
    header = re.split(r'\s{2,}', lines[2].strip())

    # Extract the rows of data
    rows = []
    for line in lines[3:]:
        if line.strip() == '':
            continue
        # Split the line based on whitespace
        columns = re.split(r'\s{2,}', line.strip())
        if len(columns) == len(header):
            rows.append(columns)
        else:
            # Handle cases where the security name has spaces
            combined_columns = []
            i = 0
            while i < len(columns):
                if len(combined_columns) < len(header) - 1:
                    combined_columns.append(columns[i])
                else:
                    combined_columns[-1] += ' ' + columns[i]
                i += 1
            rows.append(combined_columns)

    # Create the DataFrame
    df = pd.DataFrame(rows, columns=header)

    return df

def fetch_short_data(supabase, today):
    date = today.strftime("%Y%m%d")

    if today.month < 10:
        month = f"0{today.month}"
    else:
        month = today.month
        
    try:
        url = f'https://api2.sgx.com/sites/default/files/reports/short-sell/{today.year}/{month}/website_DailyShortSell{date}1815.txt'
        short_url = requests.get(url)
        text_data = short_url.text
        data = extract_txt(text_data)
        data['date'] = today 
        data["date"] = pd.to_datetime(data["date"])
        data = data[~data["ShortSaleValue"].isna()]
    except:
        print(f'Error in {today}')
        sys.exit(1)
        

    for i in data.columns:
        if type(data[i]) == "str":
            data[i] = data[i].str.replace('$', '').str.strip()

    data.columns = ['security','volume','currency','value','date']

    # Get Symbol for each Company
    df_sgx = supabase.table("sgx_companies").select("symbol","name","market_cap").execute()
    df_sgx = pd.DataFrame(df_sgx.data)

    data["security"] = data["security"].str.lower()

    # Using the fuzz (see other file...)
    data["security"] = data["security"].str.replace('$', '').str.strip()
    data['symbol'] = None # Make new column to be inserted
    data = data[["security",'date','volume','value']].rename(columns={"security":"name"})
    companies_dict_list = df_sgx[['symbol','name']].to_dict(orient="records")

    unique_value_short_sell = data['name'].unique()
    cleaned_unique_value_short_sell = preprocess_names(unique_value_short_sell)
    list_of_dictionaries = match_names(cleaned_unique_value_short_sell, unique_value_short_sell, companies_dict_list)
    final_data, still_null_data = vote_names(list_of_dictionaries)
    # save_names(final_data, still_null_data) # Only if needed
    df_final = insert_names_to_df(final_data, data)

    df_top_sgx = df_sgx.sort_values("market_cap", ascending=False).head(50)
    df_csv = df_final[~df_final.symbol.isin(df_top_sgx.symbol.unique())]
    df_final = df_final[df_final.symbol.isin(df_top_sgx.symbol.unique())]
    return df_final, df_csv

def delete_old_data(supabase,today,df_csv):
    # Delete more than 1 year data from DB and add to flat file
    sgx_short_df = pd.DataFrame(supabase.table("sgx_short_sell").select("*").lt("date",today - timedelta(365*2)).execute().data)
    sgx_short_df = pd.concat([sgx_short_df,df_csv])
    if sgx_short_df.shape[0] > 0:
        try:
            curr_short_df = pd.read_csv("historical_sgx_short_sell_data.csv")
            df_flat_file = pd.concat([curr_short_df,sgx_short_df])
        except:
            df_flat_file = sgx_short_df
            print("no prior csv file")
        df_flat_file = df_flat_file.to_csv("historical_sgx_short_sell_data.csv", index=False)
        

    supabase.table("sgx_short_sell").delete().lt("date",today - timedelta(365*2)).execute()

def insert_data_to_db(df_fuzzy,supabase, today):
    
    df_fuzzy = df_fuzzy.replace({np.nan: None})
    df_fuzzy["date"] = df_fuzzy["date"].astype('str')
    
    # Insert New Data
    for row in range(0,df_fuzzy.shape[0]):
        try:
            supabase.table("sgx_short_sell").insert(dict(df_fuzzy.iloc[row])).execute()

        except:
            logging.error(f"Failed to update description for row {row} in date {today}.")

def main():
    parser = argparse.ArgumentParser(description="Update short sell date to be fetched")
    parser.add_argument('date', type=str, help='Specify the date of shortsell format "YYYYMMDD", if today specify "today"')

    args = parser.parse_args()
    
    if args.date == "today":
        # Fetch Daily Short Sell Data
        today = datetime.today()
    else:
        today = datetime.strptime(args.date,"%Y%m%d")

    supabase = create_client(os.getenv("SUPABASE_URL"),os.getenv("SUPABASE_KEY"))
    
    df_final,df_csv = fetch_short_data(supabase,today)

    print(df_final)
    delete_old_data(supabase,today,df_csv)
    # insert_data_to_db(df_final, supabase, today)

def initiate_logging(LOG_FILENAME):
    reload(logging)

    formatLOG = '%(asctime)s - %(levelname)s: %(message)s'
    logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO, format=formatLOG)
    logging.info('Program started')

if __name__ == "__main__":
    LOG_FILENAME = 'scraper.log'
    initiate_logging(LOG_FILENAME)

    main()

    logging.info(f"Finish scrape sgx short sell data")



