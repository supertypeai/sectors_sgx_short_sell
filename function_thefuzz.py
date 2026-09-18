import pandas as pd
from supabase import create_client
import os
import re
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

# Legal/REIT/currency words trimmed before comparing names.
# Longest first so "realestateinvestmenttrust" goes before "trust".
_CANONICAL_SUFFIXES = tuple(sorted({
  "realestateinvestmenttrust", "realestatetrust", "reit", "trust", "holdings",
  "holding", "limited", "corporation", "company", "group", "ltd", "plc",
  "pcl", "inc", "grp", "tr", "sgd", "usd", "eur", "gbp", "hkd", "cny",
  "jpy", "myr", "aud", "bhd", "berhad",
}, key=len, reverse=True))

# Tokens the SGX report abbreviates, expanded before canonicalising so
# "intl cement" and "International Cement Group" land on the same key.
_ABBREVIATIONS = {
  "intl": "international",
  "gbl": "global",
  "grp": "group",
  "hldg": "holding",
  "hldgs": "holdings",
  "envres": "environmentalresources",
}

# Standalone currency/country tokens are noise: the same trust appears as
# "Keppel Pacific Oak US REIT" (company) and "KepPacOakReitUSD" (report), so
# "us" must go too or those two keys never meet. Verified against the live
# company list: only Manulife/Prime/Keppel-Pacific-Oak hold the affected keys.
_CURRENCY_TOKENS = re.compile(r'\b(?:usd|sgd|eur|gbp|hkd|cny|jpy|myr|aud|us|hk)\b')
# ...and the report also glues the currency onto the name with no separator.
_GLUED_CURRENCY = re.compile(r'(?:usd|sgd|eur|gbp|hkd|cny|jpy|myr|aud)$')
# A stripped key shorter than this is more likely to collide than to match.
_MIN_CANONICAL_LEN = 4

def canonical_name(value: str) -> str:
  """Collapse a security name to a comparable key: lowercase, expand common
  abbreviations, alphanumerics only, drop a leading article, then strip trailing
  legal/REIT/currency words (repeat until stable)."""
  if value is None or not isinstance(value, str):
    return ""
  lowered = value.lower()
  for abbr, full in _ABBREVIATIONS.items():
    lowered = re.sub(rf'\b{abbr}\b', full, lowered)
  lowered = _CURRENCY_TOKENS.sub(' ', lowered)
  cleaned = re.sub(r'[^a-z0-9]', '', lowered)
  if cleaned.startswith("the") and len(cleaned) > 3:
    cleaned = cleaned[3:]
  stripped = _GLUED_CURRENCY.sub('', cleaned)
  if len(stripped) >= _MIN_CANONICAL_LEN:
    cleaned = stripped
  changed = True
  while changed:
    changed = False
    for suffix in _CANONICAL_SUFFIXES:
      if cleaned.endswith(suffix) and len(cleaned) > len(suffix):
        cleaned = cleaned[: -len(suffix)]
        changed = True
  return cleaned

# Yahoo sometimes omits shortName for a company that shares its legal name with
# another listing (Z74/Z77 both "Singapore Telecommunications Ltd"). These
# explicit fallbacks restore the exchange short name and feed both match passes.
SHORT_NAME_FALLBACKS = {
  "Z74.SI": "Singtel",
}

_CURRENCIES = {"SGD", "USD", "EUR", "GBP", "HKD", "CNY", "JPY", "MYR", "AUD"}

def _report_currency(name: str) -> str | None:
  """Trailing currency token of a report name ("hph trust usd" -> USD)."""
  tokens = re.findall(r'[a-z]+', str(name).lower())
  return tokens[-1].upper() if tokens and tokens[-1].upper() in _CURRENCIES else None

def _alias_score(name: str, aliases: list) -> int:
  """Same four-metric vote score used by match_names, best across aliases."""
  best = 0
  for alias in aliases:
    if not alias:
      continue
    left, right = name.upper(), alias.upper()
    best = max(best, fuzz.partial_ratio(left, right) + fuzz.token_sort_ratio(left, right)
               + fuzz.token_set_ratio(left, right) + fuzz.partial_token_sort_ratio(left, right))
  return best

def resolve_symbols(df_short_sell: pd.DataFrame, companies_dict_list: list) -> tuple:
  """Map report names to symbols.

  Returns ``(df_matched, final_data, unmatched_names, ambiguous_names)``.
  Resolution order, each step only seeing what the previous left over:

  1. exact canonical hit on ``short_name`` or legal ``name`` (unique -> taken);
  2. currency token in the report name vs ``sgx_companies.currency``;
  3. ``is_active`` when exactly one candidate is active;
  4. fuzzy vote over legal names only (``short_name`` is often truncated, e.g.
     ``'ES'``, and substring-matches the wrong company);
  5. elimination, when a sibling symbol is already claimed in this batch.

  Unmatched and ambiguous names are returned rather than guessed.
  """
  canonical_map = {}
  for company in companies_dict_list:
    for alias in (company.get("short_name"), company.get("name"),
                  SHORT_NAME_FALLBACKS.get(company["symbol"])):
      key = canonical_name(alias)
      if key:
        canonical_map.setdefault(key, set()).add(company["symbol"])

  # Fuzzy candidates use the legal name (+ fallbacks) only, never short_name:
  # truncated short_names like "ES" or "Global Inv" substring-match and out-score
  # the right answer ("china envres"->ES, "g invacom"->Global Inv). Exact
  # canonical matching above still uses short_name, where it is unambiguous.
  alias_dict_list = []
  for c in companies_dict_list:
    for alias in (c["name"], SHORT_NAME_FALLBACKS.get(c["symbol"])):
      if alias:
        alias_dict_list.append({"symbol": c["symbol"], "name": alias})

  unmatched_empty = []

  final_data = {}
  ambiguous = {}
  remaining = []
  unique_names = list(df_short_sell['name'].unique())
  cleaned_map = dict(zip(unique_names, preprocess_names(unique_names)))
  for name in unique_names:
    # Try the raw report text first, then the preprocessed form: the
    # special_cases table rewrites exchange shorthand ("keppacoakreitusd",
    # "g invacom") into something the company names can be compared against.
    raw_key = canonical_name(name)
    pre_key = canonical_name(cleaned_map[name])
    symbols = canonical_map.get(raw_key) if raw_key else None
    if not symbols and pre_key:
      symbols = canonical_map.get(pre_key)
    if not symbols:
      # A name with no alphanumerics (blank/''/'$') would fuzzy-match everything
      # at 100 and land on an arbitrary company - report it, never guess.
      if not raw_key and not pre_key:
        unmatched_empty.append(name)
      else:
        remaining.append(name)
    elif len(symbols) == 1:
      final_data[name] = {"symbol": next(iter(symbols)), "name": name, "value": 100}
    else:
      ambiguous[name] = symbols

  cleaned_remaining = [cleaned_map[n] for n in remaining]
  fuzzy_final, unmatched = vote_names(
    match_names(cleaned_remaining, remaining, alias_dict_list)
  )
  final_data.update(fuzzy_final)
  unmatched = unmatched_empty + unmatched

  # Same canonical key spread over several symbols (dual listings/renames):
  # prefer the currency the report states, then let fuzzy pick, but only when
  # it wins outright.
  unresolved_ambiguous = {}
  for name, symbols in ambiguous.items():
    candidates = [c for c in companies_dict_list if c["symbol"] in symbols]
    report_currency = _report_currency(name)
    if report_currency:
      currency_match = [c for c in candidates
                        if (c.get("currency") or "").upper() == report_currency]
      if currency_match:
        candidates = currency_match
    active = [c for c in candidates if c.get("is_active")]
    if len(active) == 1:
      candidates = active
    scored = sorted(
      ((_alias_score(name, [c["name"], SHORT_NAME_FALLBACKS.get(c["symbol"])]), c["symbol"])
       for c in candidates),
      reverse=True,
    )
    if len(scored) == 1 or (scored and scored[0][0] > scored[1][0]):
      final_data[name] = {"symbol": scored[0][1], "name": name, "value": scored[0][0]}
    else:
      unresolved_ambiguous[name] = symbols

  # Ambiguity by elimination: if every other candidate is already taken by a
  # different name in this batch, the leftover one is right (Keppel -> BN4 once
  # "Keppel Reit" has claimed K71U).
  assigned = {entry["symbol"] for entry in final_data.values()}
  still_ambiguous = {}
  for name, symbols in unresolved_ambiguous.items():
    leftover = symbols - assigned
    if len(leftover) == 1:
      symbol = next(iter(leftover))
      final_data[name] = {"symbol": symbol, "name": name}
      assigned.add(symbol)
    else:
      still_ambiguous[name] = symbols
  unresolved_ambiguous = still_ambiguous

  df_filled = insert_names_to_df(final_data, df_short_sell)
  print(f"[PROGRESS] Resolved {len(final_data)} names, {len(unmatched)} unmatched, "
        f"{len(unresolved_ambiguous)} ambiguous")
  return df_filled, final_data, unmatched, unresolved_ambiguous


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



if __name__ == "__main__":
  # Self-check: no DB needed. Pins the cases that regressed during development.
  for alias, report in [
    ("UOB", "uob"), ("OCBC Bank", "ocbc bank"), ("Golden Agri-Res", "golden agri-res"),
    ("YZJ Shipbldg", "yzj shipbldg sgd"), ("Seatrium", "seatrium ltd"),
    ("Suntec Real Estate Investment Trust", "suntec reit"), ("DBS", "dbs"),
    ("International Cement Group Ltd", "intl cement"),
  ]:
    assert canonical_name(alias) == canonical_name(report), (alias, report)

  companies = [
    {"symbol": "U11.SI", "name": "United Overseas Bank Ltd", "short_name": "UOB"},
    {"symbol": "U10.SI", "name": "UOB-Kay Hian Holdings Ltd", "short_name": None},
    {"symbol": "BN4.SI", "name": "Keppel Ltd", "short_name": "Keppel"},
    {"symbol": "K71U.SI", "name": "Keppel REIT", "short_name": "Keppel"},
    # truncated short_name: must NOT win the fuzzy pass over the legal name
    {"symbol": "QS9.SI", "name": "Global Invacom Group Ltd", "short_name": "G Inva"},
    {"symbol": "B73.SI", "name": "Global Investments Ltd", "short_name": "Global Inv"},
    {"symbol": "5RC.SI", "name": "ES Group (Holdings) Ltd", "short_name": "ES"},
    {"symbol": "UIX.SI", "name": "China Environmental Resources Group Ltd", "short_name": None},
    {"symbol": "CMOU.SI", "name": "Keppel Pacific Oak US REIT", "short_name": "Korereit"},
    {"symbol": "KUO.SI", "name": "International Cement Group Ltd", "short_name": None},
    {"symbol": "NIO.SI", "name": "NIO Inc", "short_name": None},
  ]
  cases = {
    "uob": "U11.SI", "uob kay hian": "U10.SI",
    "keppel": "BN4.SI", "keppel reit": "K71U.SI",
    "g invacom^": "QS9.SI", "g invacom": "QS9.SI",
    "china envres": "UIX.SI", "keppacoakreitusd": "CMOU.SI",
    "nio inc. usd ov": "NIO.SI", "intl cement": "KUO.SI",
  }
  df = pd.DataFrame({"name": list(cases), "symbol": [None] * len(cases)})
  filled, _, _, _ = resolve_symbols(df, companies)
  got = dict(zip(filled["name"], filled["symbol"]))
  assert got == cases, {k: (got.get(k), v) for k, v in cases.items() if got.get(k) != v}
  print("[SELF-CHECK] resolve_symbols ok")
