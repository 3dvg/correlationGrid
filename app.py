import sqlite3
from sqlite3 import Error
import datetime as dt
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import quandl

quandl.ApiConfig.api_key = 'YOUR API KEY HERE' 
quandl_codes = {
    '10Y T-Note':'CHRIS/CME_TY1',
    '5Y T-Note':'CHRIS/CME_FV1',
    '2Y T-Note':'CHRIS/CME_TU1',
    '30Y T-Bond':'CHRIS/CME_US1',
    'Ultra 10Y T-Note':'CHRIS/CME_TN1',
    'Ultra T-Bond':'CHRIS/CME_UL1',
    'S&P 500 E-Mini':'CHRIS/CME_ES1',
    'Nasdaq 100 E-Mini':'CHRIS/CME_NQ1',
    'Dow Futures Mini':'CHRIS/CME_YM1',
    'Crude Oil WTI':'CHRIS/CME_CL1',
    'Gold':'CHRIS/CME_GC1',
    'Silver':'CHRIS/CME_SI1',
    'Nat Gas':'CHRIS/CME_NG1',
    'Sugar':'CHRIS/ICE_SB1',
    'Corn':'CHRIS/CME_C1',
    'Eurodollar':'CHRIS/CME_ED1',
    'Euro FX':'CHRIS/CME_EC1',
    'Japanese Yen':'CHRIS/CME_JY1',
    'Aus Dollar':'CHRIS/CME_AD1'
}

DB_NAME = "futures_data.db" 
def create_database(db, futures):
    conn= sqlite3.connect(db)
    c = conn.cursor()
    for fut in futures:
        try:
            c.execute("CREATE TABLE IF NOT EXISTS '{}' (Date DATETIME PRIMARY KEY, Open FLOAT, High FLOAT, Low FLOAT, Close FLOAT)".format(fut))
        except Error as e:
            print("Error {} for {}".format(e,fut))
    conn.commit()
    # to check everything is correct uncomment the next 3 lines
    # c.execute("SELECT COUNT(name) FROM sqlite_master WHERE type='table';") 
    # print("n tables: ", c.fetchall())
    # print("n futures: ", len(futures))
    conn.close()

def insert_data(db,futures):
    conn = sqlite3.connect(db)
    c = conn.cursor()
    start, end = dt.datetime(2000, 1, 1), dt.datetime(2020, 1, 1)
    for fut, code in futures.items():
        df = quandl.get(code, start_date=start, end_date=end)
        try:
            for idx, val in df.iterrows():
                c.execute(
                    "INSERT INTO '{}' (Date, Open, High, Low, Close) VALUES(:Date, :Open, :High, :Low, :Close)".format(fut),
                    {'Date':idx.strftime('%Y-%m-%d %H:%M:%S'),'Open':val['Open'],'High':val['High'],'Low':val['Low'],'Close':val['Settle']})
            print("Inserted data for", fut)
        except Error as e:
            print(e)
    conn.commit()
    conn.close()

def compile_dfs(db, futures, start_date, end_date):
  conn = sqlite3.connect(db) 
  dfs = pd.DataFrame()
  for fut in futures:
    query="SELECT * FROM '{}' WHERE Date BETWEEN '{}' AND '{}'".format(fut,start_date,end_date)
    df = pd.read_sql_query(query, con=conn) 
    df.set_index('Date', inplace=True)
    df = df[['Close']] 
    df.rename(columns={'Close': fut}, inplace=True)
    dfs = dfs.join(df, how='outer')
  conn.close()
  return dfs

def visualize(compiled_df,start_date, end_date):
  df_corr = compiled_df.pct_change().corr() 
  data = df_corr.values
  fig = plt.figure()
  ax = fig.add_subplot(1, 1, 1)
  # Format title date ------------------------------------------
  dt_start = dt.datetime.strptime(start_date,"%Y-%m-%d %H:%M:%S")
  dt_end = dt.datetime.strptime(end_date,"%Y-%m-%d %H:%M:%S")
  title_start, title_end = str(dt_start.year), str(dt_end.year)
  if dt_start.month > 1:
    title_start += "/{}".format(dt_start.month)
    if dt_start.day > 1:
        title_start += "/{}".format(dt_start.day)
  if dt_end.month > 1:
    title_end += "/{}".format(dt_end.month)
    if dt_end.day > 1:
        title_end += "/{}".format(dt_end.day)
  plt.title('Correlations Futures ({} - {})'.format(title_start, title_end))
  # Heatmap color -----------------------------------------------
  heatmap = ax.pcolor(data, cmap='RdYlGn')
  fig.colorbar(heatmap)
  # Label position and size -------------------------------------
  ax.set_xticks(np.arange(data.shape[0]) + 0.5, minor=False)
  ax.set_yticks(np.arange(data.shape[1]) + 0.5, minor=False)
  ax.invert_yaxis()
  ax.tick_params(axis = 'both', which = 'major', labelsize = 6)
  # Set col and row names ----------------------------------------
  ax.set_xticklabels(df_corr.columns)
  ax.set_yticklabels(df_corr.index)
  # Rotate x axis labels -----------------------------------------
  plt.xticks(rotation=90)
  # Heatmap max and min values -----------------------------------
  heatmap.set_clim(-1, 1)

  plt.tight_layout()
  plt.show()

if __name__ == "__main__":
    # Uncomment these 2 lines if it's the first time you run the code
    # create_database(DB_NAME,quandl_codes)
    # insert_data(DB_NAME,quandl_codes)

    start_date = '2019-01-01 00:00:00'
    end_date = '2020-01-01 00:00:00'
    compiled_data = compile_dfs(DB_NAME, quandl_codes, start_date, end_date)
    visualize(compiled_data,start_date, end_date)