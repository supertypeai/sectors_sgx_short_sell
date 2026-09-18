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
from function_thefuzz import resolve_symbols

load_dotenv()

# SGX symbols are stored with a ".SI" suffix (e.g. "A17U.SI")
SYMBOL_SUFFIX = ".SI"

def add_symbol_suffix(symbols):
    """Append ".SI" to SGX symbols. Idempotent: symbols that already carry the
    suffix (or are null) are left untouched."""
    symbols = symbols.astype("object").where(symbols.notna(), None)
    return symbols.map(
        lambda symbol: symbol
        if symbol is None or str(symbol).strip().upper().endswith(SYMBOL_SUFFIX)
        else f"{str(symbol).strip()}{SYMBOL_SUFFIX}"
    )

def extract_txt(text_data):
    
    # Split the content into linesp
    lines = text_data.split('\r\n')

    # Extract the header
    header = re.split(r'\s{2,}', lines[2].strip())

    # Extract the rows of data
    rows = []
    for line in lines[3:]:
        if line.strip() == '':
            continue
        # Footer separator: everything after it (TOTAL / currency totals) is not data
        if set(line.strip()) == {'='}:
            break
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
    df_sgx = supabase.table("sgx_companies").select("symbol","name","short_name","currency","is_active").execute()
    df_sgx = pd.DataFrame(df_sgx.data)
    df_sgx["symbol"] = add_symbol_suffix(df_sgx["symbol"])

    # Only needed for the top-70-by-mcap cap
    # latest_date = (
    # supabase.table("sgx_daily_data")
    # .select("date")
    # .order("date", desc=True)
    # .limit(1)
    # .execute()
    # .data[0]["date"]
    # )
    #
    # df_sgx_daily = (
    #     supabase.table("sgx_daily_data")
    #     .select("symbol", "market_cap")
    #     .eq("date", latest_date)
    #     .execute()
    # )
    # df_sgx_daily = pd.DataFrame(df_sgx_daily.data)
    # df_sgx_daily = df_sgx_daily.dropna()
    # df_sgx_daily["symbol"] = add_symbol_suffix(df_sgx_daily["symbol"])

    data["security"] = data["security"].str.lower()

    # Using the fuzz (see other file...)
    data["security"] = data["security"].str.replace('$', '').str.strip()
    data['symbol'] = None # Make new column to be inserted
    data = data[["security",'date','volume','value']].rename(columns={"security":"name"})
    companies_dict_list = df_sgx[['symbol','name','short_name','currency','is_active']].to_dict(orient="records")

    df_final, _, unmatched_names, ambiguous_names = resolve_symbols(data, companies_dict_list)

    # df_top_sgx = df_sgx_daily.sort_values("market_cap", ascending=False).head(70)
    # df_csv = df_final[~df_final.symbol.isin(df_top_sgx.symbol.unique())]
    # df_final = df_final[df_final.symbol.isin(df_top_sgx.symbol.unique())]
    # return df_final, df_csv
    return df_final, {"unmatched": unmatched_names, "ambiguous": ambiguous_names}

def delete_old_data(supabase,today):
    # Delete more than 2 year data from DB and add to flat file
    sgx_short_df = pd.DataFrame(supabase.table("sgx_short_sell").select("*").lt("date",today - timedelta(365*2)).execute().data)
    if sgx_short_df.shape[0] > 0:
        sgx_short_df["symbol"] = add_symbol_suffix(sgx_short_df["symbol"])
        try:
            curr_short_df = pd.read_csv("historical_sgx_short_sell_data.csv")
            curr_short_df["symbol"] = add_symbol_suffix(curr_short_df["symbol"])
            df_flat_file = pd.concat([curr_short_df,sgx_short_df])
        except:
            df_flat_file = sgx_short_df
            print("no prior csv file")
        df_flat_file = df_flat_file.to_csv("historical_sgx_short_sell_data.csv", index=False)
        

    supabase.table("sgx_short_sell").delete().lt("date",today - timedelta(365*2)).execute()

def insert_data_to_db(df_fuzzy,supabase, today):
    
    df_fuzzy = df_fuzzy.replace({np.nan: None})
    df_fuzzy["date"] = df_fuzzy["date"].astype('str')
    df_fuzzy["symbol"] = add_symbol_suffix(df_fuzzy["symbol"])

    # Insert New Data (single batched call: one round-trip, atomic)
    records = df_fuzzy.to_dict(orient="records")
    try:
        supabase.table("sgx_short_sell").insert(records).execute()
    except Exception as e:
        logging.error(f"Insert failed for {len(records)} rows on {today}: {e}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="Update short sell date to be fetched")
    parser.add_argument('date', type=str, help='Specify the date of shortsell format "YYYYMMDD", if today specify "today"')
    parser.add_argument('--dry-run', action='store_true', help='Resolve and report only; never write to the database')

    args = parser.parse_args()

    if not args.dry_run:
        initiate_logging('scraper.log')
    
    if args.date == "today":
        # Fetch Daily Short Sell Data
        today = datetime.today()
    else:
        today = datetime.strptime(args.date,"%Y%m%d")

    supabase = create_client(os.getenv("SUPABASE_URL"),os.getenv("SUPABASE_KEY"))
    
    # df_final,df_csv = fetch_short_data(supabase,today)
    df_final, diagnostics = fetch_short_data(supabase,today)

    print(df_final)
    print(f"[SUMMARY] mapped rows: {df_final.shape[0]}; "
          f"unmatched names: {len(diagnostics['unmatched'])} {diagnostics['unmatched']}; "
          f"ambiguous names: {len(diagnostics['ambiguous'])} {diagnostics['ambiguous']}")

    if args.dry_run:
        print("[DRY-RUN] no database writes performed")
        return

    delete_old_data(supabase,today)
    insert_data_to_db(df_final, supabase, today)

def initiate_logging(LOG_FILENAME):
    reload(logging)

    formatLOG = '%(asctime)s - %(levelname)s: %(message)s'
    logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO, format=formatLOG)
    logging.info('Program started')

if __name__ == "__main__":
    main()

    logging.info(f"Finish scrape sgx short sell data")



