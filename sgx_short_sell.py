import requests
import pandas as pd
import numpy as np
import re
from supabase import create_client
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
# from fuzzywuzzy import fuzz, process
import logging
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

# def fuzzy_merge(df_1, df_2, key1, key2, threshold=90, limit=1):
#     """
#     :param df_1: the left table to join
#     :param df_2: the right table to join
#     :param key1: key column of the left table
#     :param key2: key column of the right table
#     :param threshold: how close the matches should be to return a match, based on Levenshtein distance
#     :param limit: the amount of matches that will get returned, these are sorted high to low
#     :return: dataframe with boths keys and matches
#     """
#     s = df_2[key2].tolist()
    
#     m = df_1[key1].apply(lambda x: process.extract(x, s, limit=limit))    
#     df_1['matches'] = m
    
#     m2 = df_1['matches'].apply(lambda x: ', '.join([i[0] for i in x if i[1] >= threshold]))
#     df_1['matches'] = m2
    
#     return df_1

def fetch_short_data(supabase, today):
    date = today.strftime("%Y%m%d")

    if today.month < 10:
        month = f"0{today.month}"
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
            print(data[i])
            data[i] = data[i].str.replace('$', '').str.strip()

    data.columns = ['security','volume','currency','value','date']

    # Get Symbol for each Company
    df_sgx = supabase.table("sgx_companies").select("symbol","name").execute()
    df_sgx = pd.DataFrame(df_sgx.data)

    data["security"] = data["security"].str.lower()
    # df_sgx['name'] = df_sgx['name'].str.lower()

    # Using FuzzyWuzzy
    # df_fuzzy = fuzzy_merge(data, df_sgx, 'security', 'name', threshold=90)
    # df_fuzzy = df_fuzzy[["security",'date','volume','value','matches']].rename(columns={"matches":"name"})
    # df_fuzzy = df_fuzzy.merge(df_sgx, on=["name"], how="left").drop(["name"],axis=1)
    # df_fuzzy = df_fuzzy[["security","symbol",'date','volume','value']].rename(columns={"security":"name"})
    # df_fuzzy["name"] = df_fuzzy["name"].str.replace('$', '').str.strip()
    # df_fuzzy = df_fuzzy.drop_duplicates(['name',"volume",'value'])

    # Using the fuzz (see other file...)
    data["security"] = data["security"].str.replace('$', '').str.strip()
    data['symbol'] = None # Make new column to be inserted
    data = data[["security",'date','volume','value']].rename(columns={"security":"name"})
    companies_dict_list = df_sgx.to_dict(orient="records")

    unique_value_short_sell = data['name'].unique()
    cleaned_unique_value_short_sell = preprocess_names(unique_value_short_sell)
    list_of_dictionaries = match_names(cleaned_unique_value_short_sell, unique_value_short_sell, companies_dict_list)
    final_data, still_null_data = vote_names(list_of_dictionaries)
    # save_names(final_data, still_null_data) # Only if needed
    df_final = insert_names_to_df(final_data, data)

    return df_final

def delete_old_data(supabase,today):
    # Delete more than 1 year data from DB and add to flat file
    sgx_short_df = pd.DataFrame(supabase.table("sgx_short_sell").select("*").lt("date",today - timedelta(365)).execute().data)
    if sgx_short_df.shape[0] > 0:
        curr_short_df = pd.read_csv("historical_sgx_short_sell_data.csv")
        df_flat_file = pd.concat([curr_short_df,sgx_short_df])
        df_flat_file = df_flat_file.to_csv("historical_sgx_short_sell_data.csv", index=False)

    supabase.table("sgx_short_sell").delete().lt("date",today - timedelta(365)).execute()

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
    parser = argparse.ArgumentParser(description="Update sg or my data. If no argument is specified, the sg data will be updated.")
    parser.add_argument('date', type=str, help='Specify the date of shortsell format "YYYYMMDD", if today specify "today"')

    args = parser.parse_args()

    if args.date == "today":
        # Fetch Daily Short Sell Data
        today = datetime.today()
    else:
        today = datetime.strptime(args.date,"%Y%m%d")

    supabase = create_client(os.getenv("SUPABASE_URL"),os.getenv("SUPABASE_KEY"))
    
    df_final = fetch_short_data(supabase,today)
    delete_old_data(supabase,today)
    insert_data_to_db(df_final, supabase, today)

if __name__ == "__main__":
    main()

