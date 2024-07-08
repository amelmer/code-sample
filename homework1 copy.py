## Allison Elmer
import os
import pandas as pd
import numpy as np

bea_df = pd.read_csv('bea_data.csv', skiprows=[0,1,2])
# the first three rows contain a description of the table, 
# so they are skipped

bea_df_to_reshape = bea_df.drop(['GeoFips', 'LineCode'], axis=1)
bea_df_long = bea_df_to_reshape.melt(
    id_vars=['GeoName', 'Description'], value_vars=['2005', '2006', '2007'], 
    var_name='year')
    #columns '2007', '2006', and '2005' need to become one column (year)
bea_df_reshaped = bea_df_long.pivot_table(
    index=['GeoName','year'], values='value', columns='Description', 
    aggfunc='first').reset_index()
    # column 'Description' needs to become three columns 
    # ('military', 'manufacturing', and 'total')
    # Source: 
    # https://stackoverflow.com/questions/49943627/pivoting-dataframe-with-multiple-columns-for-the-index

bea_df_final = bea_df_reshaped.rename(
    columns={"GeoName":"county", 
             "      Manufacturing":"manufacturing", 
             "      Military":"military", 
             "Total employment (number of jobs)":"total"})

def recode_na(df, nan_vals):
    df = df.replace(nan_vals, pd.NA)
    # Source: 
    # https://stackoverflow.com/questions/34794067/how-to-set-a-cell-to-nan-in-a-pandas-dataframe
    return df

bea_nan_vals = ['(D)', '(NA)']
bea_df_final = recode_na(bea_df_final, bea_nan_vals)

def object_to_numeric(df, num_cols):
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors='coerce')
    # Source: 
    # https://stackoverflow.com/questions/36814100/pandas-to-numeric-for-multiple-columns
    return df

num_cols_bea = ['year', 'military', 'manufacturing', 'total']
bea_df_final = object_to_numeric(bea_df_final, num_cols_bea)


bls_df = pd.read_excel(
    'bls_data.xlxs', skiprows=[0,1,3]).rename(columns={'Area':'msa'})

def rename_cols(df):
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.replace(" ", "_", regex=True)
    return df

bls_df_final = rename_cols(bls_df)
bls_df_final = bls_df_final[['msa', 'year', 'month', 'unemployment_rate']]
bls_nan_vals = ['(n)']
bls_df_final = recode_na(bls_df_final, bls_nan_vals)
num_cols_bls = ['unemployment_rate']
bls_df_final = object_to_numeric(bls_df_final, num_cols_bls)

bls_df_to_merge = bls_df_final.groupby(['year', 'msa'], as_index=False).agg(
    {'month':'first', 'unemployment_rate':'mean'}).drop(columns='month')


fname = 'geocorr2018_2327802044.csv'
geo_df = pd.read_csv(
    os.path.join(PATH, fname), encoding='latin-1', skiprows=[1])
  # Source for encoding: 
    # https://bobbyhadz.com/blog/python-unicodedecodeerror-utf-8-codec-cant-decode-byte

#geo_df_dropped_rows = geo_df[geo_df['cbsaname10'] != '99999']
geo_df_dropped_cols = geo_df[['cntyname', 'cbsaname10']]
geo_nan_vals = ['99999']
geo_df_dropped_cols = recode_na(geo_df_dropped_cols, geo_nan_vals)
geo_df_dropped_rows = geo_df_dropped_cols.dropna()

geo_df_final = geo_df_dropped_cols.rename(
    columns={'cntyname': 'county', 
             'cbsaname10':'msa'})

geo_df_to_merge = geo_df_final.replace(
    to_replace=[
      'Metropolitan Statistical Area', 'Micropolitan Statistical Area'], 
    value='MSA', regex=True)
geo_df_to_merge['county'] = geo_df_final['county'].replace(
    to_replace=" ", value=", ", regex=True)

merge_bea_geo = bea_df_final.merge(geo_df_to_merge, how='outer', indicator=True)

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
merge_bea_geo_final = merge_bea_geo.drop(columns='_merge')

final_merge = pd.merge(
    merge_bea_geo_final, bls_df_to_merge, how='outer', on=['msa', 'year'], 
    indicator=True)
merge_test(final_merge)
  # left_only and right_only columns would need to be addressed

merged_df_final = final_merge.groupby(['year', 'msa'], as_index=False).agg(
    {'county':'first', 
    'manufacturing':sum, 
    'military':sum, 
    'total':sum, 
    'unemployment_rate':'mean'})
merged_df_final = merged_df_final.dropna().drop(columns='county')

df_2005_6 = merged_df_final[merged_df_final['year'] != 2007]
# dropping 2007 because it is not necessary for the calculations

df_2005_6['change_ur'] = df_2005_6.groupby(['msa'])[
    'unemployment_rate'].diff().fillna(df_2005_6['unemployment_rate'])
    # Source: 
    # https://stackoverflow.com/questions/51496823/subtract-row-from-another-row-of-same-column-with-pandas 
change = df_2005_6.groupby('msa')['change_ur'].transform('min')
df_2005_6['change_ur'] = np.where(df_2005_6['year'] == 2005, change, 'nan')
# using the groupby method assigns the difference in unemployment rate
# to the 2006 rows, so this reassigns it to the 2005 rows
# Source: 
# https://stackoverflow.com/questions/75169766/replace-column-values-with-smallest-value-in-group
df_2005_6['change_ur'] = pd.to_numeric(df_2005_6['change_ur'], errors='coerce')

def create_quartile(df, column_names, year):
    for name in column_names:
      df[name + '_percent'] = np.where(
        df['year'] == year, df[name]/df['total'], 'nan')
        # Source: 
        # https://stackoverflow.com/questions/19913659/how-do-i-create-a-new-column-where-the-values-are-selected-based-on-existing-col
      df[name + '_percent'] = pd.to_numeric(df[name + '_percent'], 
        errors='coerce')
      df[name + '_quartile'] = pd.qcut(df[name + '_percent'], 4, labels=False)
      # Source: 
      # https://stackoverflow.com/questions/38356156/dataframe-add-column-whose-values-are-the-quantile-number-rank-of-an-existing-c
    return df

col_names = ['military', 'manufacturing']
create_quartile(df_2005_6, col_names, 2005)

df_2005 = df_2005_6[df_2005_6['year'] == 2005]

def av_change(df, column_names):
    for name in column_names:
      av_change_ur = df.groupby(name+'_quartile').agg(
        av_change_ur = ('change_ur', 'mean')).squeeze()
        # Source: https://datatofish.com/pandas-dataframe-to-series/
      print(
        f'average change in unemployment rate for {name} employment quartiles')
      print(av_change_ur)

av_change(df_2005, col_names)
