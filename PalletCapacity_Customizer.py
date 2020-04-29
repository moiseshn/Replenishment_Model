#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
from pathlib import Path
import numpy as np


# ### Pallet Capacity Download

# In[ ]:


cap_cz = pd.read_csv('InputFiles/PalletCapacity/pallet_capacity_cz.csv')
cap_sk = pd.read_csv('InputFiles/PalletCapacity/pallet_capacity_sk.csv')
cap_hu = pd.read_csv('InputFiles/PalletCapacity/pallet_capacity_hu.csv')
cap_pl = pd.read_csv('InputFiles/PalletCapacity/pallet_capacity_pl.csv')


# In[ ]:


pal_cap_file = pd.concat([cap_cz,cap_sk,cap_hu,cap_pl])


# In[ ]:


# temporary as I've already revised SQL
pal_cap_file.columns = pal_cap_file.columns.str.replace('table1.','')


# In[ ]:


len(pal_cap_file)


# In[ ]:


pal_cap_file.columns


# In[ ]:


#pal_cap_file = pal_cap_file.groupby(['tpn', 'pmg', 'supplier_id', 'case_size', 'case_type', 'country', 'year'])['pallet_capacity'].mean().reset_index() # if all columns are the same then for now use averages for pallet capacity


# In[ ]:


pc_df = pal_cap_file.sort_values(['year', 'tpn', 'supplier_id', 'case_size', 'case_type', 'country'], ascending=False)
pc_df = pc_df.drop_duplicates(subset=['tpn', 'pmg', 'supplier_id', 'case_size', 'case_type', 'country'] ,keep='first')
pc_df = pc_df[['country', 'tpn', 'pmg', 'supplier_id', 'case_size', 'case_type', 'pallet_capacity']]


# In[ ]:


pc_df[pc_df.duplicated(['country', 'tpn', 'pmg', 'supplier_id', 'case_size', 'case_type'])]


# In[ ]:


pc_df.rename(columns={'case_size': 'case_capacity'}, inplace=True) # we change the name for comparison between 2 tables
pc_df.dropna(axis=0, how='any', inplace=True) # remove all records with at least one null


# In[ ]:


pc_df.head()


# In[ ]:


len(pc_df)


# In[ ]:


pc_df.isnull().sum()


# In[ ]:


pc_df.describe()


# In[ ]:


# making averages for those PMGs which had no sales
pc_df_avg = pc_df.groupby(['pmg'])['pallet_capacity'].mean().reset_index()
pc_df_avg.rename(columns={'pallet_capacity':'pcase_avg_pmg'}, inplace=True)


# In[ ]:


pc_df_avg.loc[pc_df_avg.pmg=='HDL46','pcase_avg_pmg'].sort_values().unique()


# In[ ]:


pc_df = pc_df[['country', 'tpn', 'supplier_id', 'case_capacity', 'case_type', 'pallet_capacity']] # we do not need pmg later (merging with sales)


# ### Combining with sales

# In[ ]:


folder = Path('C:/D/#PRACA/2020/Inputs/Database/B.Customized_Files/')


# In[ ]:


stores_and_pmg = pd.read_excel(folder / 'Dataset_Inputs.xlsx', sheet_name='stores_and_pmg')


# In[ ]:


stores_and_pmg['dep'] = stores_and_pmg.pmg.str[:3]


# In[ ]:


isold_file = pd.read_csv('InputFiles/items_sold_tpn_avg2019.zip', compression='zip')


# In[ ]:


isold_file = isold_file[['country', 'store', 'pmg', 'tpn', 'supplier_id', 'case_capacity',
                         'sold_units']]


# In[ ]:


new_isold_file = isold_file.merge(pc_df, on=['country', 'tpn', 'supplier_id', 'case_capacity'], how='left')


# In[ ]:


new_isold_file['dep'] = new_isold_file.pmg.str[:3]


# In[ ]:


# Pallet Capacity part 1
pcase_table = new_isold_file[['country', 'store', 'tpn', 'dep', 'pmg', 'pallet_capacity', 'sold_units']].copy()
pcase_table.dropna(axis=0, how='any', inplace=True)

pmg_sales_ratio = pcase_table.groupby(['country', 'store', 'pmg']).sold_units.sum().to_frame().reset_index()
pmg_sales_ratio['store'] = pmg_sales_ratio.store.astype(int)
pmg_sales_ratio = pmg_sales_ratio.rename(columns={'sold_units': 'pmg_sales'})

dep_sales_ratio = pcase_table.groupby(['country', 'store', 'dep']).sold_units.sum().to_frame().reset_index()
dep_sales_ratio['store'] = dep_sales_ratio.store.astype(int)
dep_sales_ratio = dep_sales_ratio.rename(columns={'sold_units': 'dep_sales'})

pcase_table = pcase_table.merge(pmg_sales_ratio, on=['country', 'store', 'pmg'], how='inner')
pcase_table = pcase_table.merge(dep_sales_ratio, on=['country', 'store', 'dep'], how='inner')
pcase_table['pmg_ratio'] = pcase_table.sold_units / pcase_table.pmg_sales
pcase_table['dep_ratio'] = pcase_table.sold_units / pcase_table.dep_sales
pcase_table['pcase_spmg'] = pcase_table.pallet_capacity * pcase_table.pmg_ratio
pcase_table['pcase_sdep'] = pcase_table.pallet_capacity * pcase_table.dep_ratio

pcase_table_store_dep = pcase_table.groupby(['country', 'store', 'dep']).pcase_sdep.sum().to_frame().reset_index()
pcase_table_store_pmg = pcase_table.groupby(['country', 'store', 'pmg']).pcase_spmg.sum().to_frame().reset_index()
pcase_table_pmg = pcase_table_store_pmg.groupby(['pmg']).pcase_spmg.mean().to_frame().reset_index()
pcase_table_pmg = pcase_table_pmg.rename(columns={'pcase_spmg':'pcase_pmg'})


# In[ ]:


# są pmg gdzie niezależnie od sklepu/kraju nie ma danych dla pallet capacity
new_isold_file.loc[new_isold_file.pmg=='HDL44','pallet_capacity'].sort_values().unique()


# In[ ]:


# Pallet Capacity part 2
parameters = stores_and_pmg.merge(pcase_table_store_pmg, on=['country', 'store', 'pmg'], how='left')
parameters = parameters.merge(pcase_table_store_dep, on=['country', 'store', 'dep'], how='left')
parameters = parameters.merge(pcase_table_pmg, on=['pmg'], how='left')
parameters = parameters.merge(pc_df_avg, on=['pmg'], how='left')


# In[ ]:


parameters.columns
# Priority 1: 'pcase_spmg' = pallet capacity per store / pmg (sales weighted)
# Priority 4: 'pcase_sdep' = pallet capacity per store / dep (sales weighted)
# Priority 2: 'pcase_pmg' = pallet capacity per store / pmg (sales weighted)
# Priority 3: 'pcase_avg_pmg' = pallet capacity per store / pmg (NO sales weighted!)


# In[ ]:


print(len(parameters),'\n')
print(parameters.isnull().sum())


# In[ ]:


stores_and_pmg.head()


# In[ ]:


parameters.head()


# In[ ]:


parameters['pcase'] = np.where(parameters.pcase_spmg>0,parameters.pcase_spmg,0)
parameters.pcase = np.where(((parameters.pcase==0)&(parameters.pcase_pmg>0)),parameters.pcase_pmg,parameters.pcase)
parameters.pcase = np.where(((parameters.pcase==0)&(parameters.pcase_avg_pmg>0)),parameters.pcase_avg_pmg,parameters.pcase)
parameters.pcase = np.where(((parameters.pcase==0)&(parameters.pcase_sdep>0)),parameters.pcase_sdep,parameters.pcase)
parameters.drop(columns={'pcase_spmg','pcase_sdep','pcase_pmg','pcase_avg_pmg'}, inplace=True)


# In[ ]:


parameters.describe()


# In[ ]:


pallet_capacity_df = pc_df_avg.merge(parameters, on='pmg', how='right')


# In[ ]:


pallet_capacity_df.isnull().sum()


# In[ ]:


pallet_capacity_df['checking'] = np.where((pallet_capacity_df.pcase_avg_pmg>pallet_capacity_df.pcase), pallet_capacity_df.pcase/pallet_capacity_df.pcase_avg_pmg, pallet_capacity_df.pcase_avg_pmg/pallet_capacity_df.pcase)


# In[ ]:


total_rows = len(pallet_capacity_df)
weird_rows = len(pallet_capacity_df[pallet_capacity_df.checking<0.2])
weird_rows/total_rows


# In[ ]:


pallet_capacity_df['Pallet_Capacity'] = np.where(pallet_capacity_df.checking<=0.2, pallet_capacity_df.pcase_avg_pmg, pallet_capacity_df.pcase)


# In[ ]:


pallet_capacity_df.sort_values('pcase', ascending=False).head(20)


# In[ ]:


pallet_capacity_df[(pallet_capacity_df.Pallet_Capacity>=2000)|(pallet_capacity_df.Pallet_Capacity<5)]


# In[ ]:


pallet_capacity_df = pallet_capacity_df[['country', 'store', 'pmg', 'Pallet_Capacity']]


# In[ ]:


pallet_capacity_df.to_csv('Pallet_Capacity.csv',index=False)

