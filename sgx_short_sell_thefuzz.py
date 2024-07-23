import requests
import pandas as pd
import numpy as np
import re
from supabase import create_client
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from thefuzz import fuzz
from thefuzz import process
import logging
logging.basicConfig(level=logging.ERROR)
import argparse
import sys

load_dotenv()

if __name__ == "__main__":
  url_supabase = os.getenv("SUPABASE_URL")
  key = os.getenv("SUPABASE_KEY")
  supabase = create_client(url_supabase, key)

  # Get the table
  db_data = supabase.table("idx_key_stats").select("").execute()
  df_db_data = pd.DataFrame(db_data.data)
