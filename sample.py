import os
import pandas as pd
import numpy as np

# Clean Bureau of Labor Statistics Data
def clean_bls_df(fname):
    # read in data 
    df = pd.read_excel(fname, 
    skiprows=[0,1,3]).rename(columns={'Area':'msa'})

    # clean column headers & remove unnecessary columns
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.replace(" ", "_", regex=True)
    df = df[['msa', 'year', 'month', 'unemployment_rate']]

    # recode na values & convert numbers to numeric
    nan_vals = ['(n)']
    df = df.replace(nan_vals, pd.NA)
    num_cols = ['unemployment_rate']
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors='coerce')

    df_to_merge = df.groupby(['year', 'msa'], as_index=False).agg(
    {'month':'first', 'unemployment_rate':'mean'}).drop(columns='month')

    return df_to_merge

bls_fname = 'bls_data.xlsx'
bls_data = clean_bls_df(bls_fname)

# Clean Bureau of Economic Analysis Data
def clean_bea_df(fname):
    # read in data
    df = pd.read_csv(fname, skiprows=[0,1,2])

    # reshape
    df = df.drop(['GeoFips', 'LineCode'], axis=1)
    df = df.melt(
      id_vars=['GeoName', 'Description'], value_vars=['2005', '2006', '2007'],
      var_name='year')
    df = df.pivot_table(
      index=['GeoName', 'year'], values='value', columns='Description',
      aggfunc='first').reset_index()

    # rename columns
    df = df.rename(
      columns={'GeoName':'county',
      "      Manufacturing":"manufacturing",
      "      Military":"military",
      "Total employment (number of jobs)":"total"}
    )

    # recode na values
    nan_vals = ['(D)', '(NA)']
    df = df.replace(nan_vals, pd.NA)

    # convert cols to numeric
    num_cols = ['year', 'military', 'manufacturing', 'total']
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors='coerce')

    return df

bea_fname = 'bea_data.csv'
bea_data = clean_bea_df(bea_fname)

# Clean Geo Data
def clean_geo_df(fname):
    # read in dataset
    df = pd.read_csv(fname, encoding='latin-1', skiprows=[1])
    df = df[['cntyname', 'cbsaname10']]

    # recode & drop na values
    nan_vals = ['99999']
    df = df.replace(nan_vals, pd.NA)
    df = df.dropna()

    # rename columns
    df = df.rename(
      columns={'cntyname': 'county', 'cbsaname10':'msa'}
    )

    # replace values
    df = df.replace(
      to_replace=[
      'Metropolitan Statistical Area', 'Micropolitan Statistical Area'], value='MSA', regex=True)
    df['county'] = df['county'].replace(to_replace=" ", value=", ", regex=True)

    return df

geo_fname = 'geo_data.csv'
geo_data = clean_geo_df(geo_fname)

# merge datasets
merge_bea_geo = bea_data.merge(geo_data, how='outer', indicator=True)

def merge_test(new_dataframe):
    test = new_dataframe[new_dataframe['_merge'] != 'both']
    if len(test) == 0:
      print("Merged as expected")
    if len(test) != 0:
      print("Unexpected merge")
      print(test)
      # Source for len(test): 
      # https://stackoverflow.com/questions/19828822/how-to-check-whether-a-pandas-dataframe-is-empty

merge_test(merge_bea_geo)
merge_bea_geo = merge_bea_geo.drop(columns='_merge')

final_merge = pd.merge(
    merge_bea_geo, bls_data, how='outer', on=['msa', 'year'], 
    indicator=True)
merge_test(final_merge)
  # left_only and right_only columns need to be addressed

final_df = final_merge.groupby(['year', 'msa'], as_index=False).agg(
    {'county':'first', 
    'manufacturing':sum, 
    'military':sum, 
    'total':sum, 
    'unemployment_rate':'mean'})
final_df = final_df.dropna().drop(columns='county')

# calculate average unemployment rate changes
df_calc = final_df[final_df['year'] != 2007]

def calculate_ur_change(df):
    df['change_ur'] = df.groupby(['msa'])[
      'unemployment_rate'].diff().fillna(df_calc['unemployment_rate'])
    change = df.groupby('msa')['change_ur'].transform('min')
    df['change_ur'] = np.where(df['year'] == 2005, change, pd.NA)
    df['change_ur'] = pd.to_numeric(df['change_ur'], errors='coerce')

    return df

df_calc = calculate_ur_change(df_calc)

def create_quartile(df, column_names, year):
    for name in column_names:
      df[name + '_percent'] = np.where(
        df['year'] == year, df[name]/df['total'], 'nan')
      df[name + '_percent'] = pd.to_numeric(df[name + '_percent'], 
        errors='coerce')
      df[name + '_quartile'] = pd.qcut(df[name + '_percent'], 4, labels=False)
    return df

col_names = ['military', 'manufacturing']
create_quartile(df_calc, col_names, 2005)

df_2005 = df_calc[df_calc['year'] == 2005]

def av_change(df, column_names):
    for name in column_names:
      av_change_ur = df.groupby(name+'_quartile').agg(
        av_change_ur = ('change_ur', 'mean')).squeeze()
      print(
        f'average change in unemployment rate for {name} employment quartiles')
      print(av_change_ur)

av_change(df_2005, col_names)
