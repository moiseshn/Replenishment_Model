import pandas as pd
from pathlib import Path
import numpy as np

def StoreInputsCreator(folder,inputs):
    xls = pd.ExcelFile(folder / inputs)
    pmg_df = pd.read_excel(xls, 'pmg')
    pmg_df = pmg_df[pmg_df.Area=='Replenishment']
    store_list_df = pd.read_excel(xls, 'store_list')
    capping_df = pd.read_excel(xls, 'capping')
    capping_df['is_capping_shelf'] = 1
    Dprofiles_df = pd.read_excel(xls, 'Dprofiles')
    Pprofiles_df = pd.read_excel(xls, 'Pprofiles')
    pmg_array = pmg_df.values
    store_array = store_list_df.values
    result = len(store_array) * len(pmg_array) # 871 * 154
    df_array = np.empty([result,9], dtype='object') # create an empty array
    counter = 0
    for s in range(len(store_array)):
        for p in range(len(pmg_array)):
            df_array[counter][0] = store_array[s][1] # country
            df_array[counter][1] = store_array[s][0] # store
            df_array[counter][2] = store_array[s][2] # store_name
            df_array[counter][3] = store_array[s][3] # sqm
            df_array[counter][4] = store_array[s][4] # format
            df_array[counter][5] = pmg_array[p][0] # pmg
            df_array[counter][6] = pmg_array[p][1] # pmg_name
            df_array[counter][7] = pmg_array[p][2] # dep
            df_array[counter][8] = pmg_array[p][3] # division
            counter += 1
    store_inputs = pd.DataFrame(columns=['Country', 'Store', 'Store Name', 'Plan Size', 'Format', 'Pmg', 'Pmg Name', 'Dep', 'Division'])
    store_inputs = store_inputs.append(pd.DataFrame(df_array, columns=store_inputs.columns))
    store_inputs['Dep'] = np.where(store_inputs.Pmg=='HDL01', 'NEW', store_inputs.Dep)
    store_inputs = store_inputs.merge(capping_df, on=['Store', 'Pmg'], how='left')
    store_inputs['is_capping_shelf'] = store_inputs['is_capping_shelf'].replace(np.nan,0)
    store_inputs = store_inputs.merge(Dprofiles_df, on=['Store', 'Dep'], how='left')
    store_inputs = store_inputs.merge(Pprofiles_df, on=['Country', 'Format', 'Pmg'], how='left')
    return store_inputs
'''
I needed to out an assumption in here as mainly on GM we have big numbers for pallet capacity.
From the model perspective it is better to have smaller pallet capacity value as we give more equipments and more time

We know that seize of a pallet is:
    length 120cm
    Width: 80cm
    Height: 180cm
    
Really small carton might have :
    length 20cm
    Width: 10cm
    Height: 15cm

It means that we cannot have more than 576 cartons on a pallet (80/10+120/20+180/15).
So our maximum pallet capacity cannot be bigger than 500
'''

target = 500

path = Path('C:/D/#PRACA/ReplModel_2021/') 
excel_inputs_f = 'C:/D/#PRACA/ReplModel_2021/Model_Inputs/Stores_Inputs_2021.xlsx' 
cap_cz = pd.read_csv(path/'RawData/Pallet Capacity/pallet_capacity_cz.csv')
cap_sk = pd.read_csv(path/'RawData/Pallet Capacity/pallet_capacity_sk.csv')
cap_hu = pd.read_csv(path/'RawData/Pallet Capacity/pallet_capacity_hu.csv')
cap_pl = pd.read_csv(path/'RawData/Pallet Capacity/pallet_capacity_pl.csv')
pal_cap_file = pd.concat([cap_cz,cap_sk,cap_hu,cap_pl])

# temporary as I've already revised SQL
pal_cap_file.columns = pal_cap_file.columns.str.replace('table1.','')
print(len(pal_cap_file))

pc_df = pal_cap_file.sort_values(['year', 'tpn', 'supplier_id', 'case_size', 'case_type', 'country'], ascending=False)
pc_df = pc_df.drop_duplicates(subset=['tpn', 'pmg', 'supplier_id', 'case_size', 'case_type', 'country'] ,keep='first')
pc_df = pc_df[['country', 'tpn', 'pmg', 'supplier_id', 'case_size', 'case_type', 'pallet_capacity']]
print(pc_df[pc_df.duplicated(['country', 'tpn', 'pmg', 'supplier_id', 'case_size', 'case_type'])].head()) # check do we have duplicates - should be 0
pc_df.rename(columns={'case_size': 'case_capacity'}, inplace=True) # we change the name for comparison between 2 tables
pc_df.dropna(axis=0, how='any', inplace=True) # remove all records with at least one null
print(len(pc_df))
print(pc_df.isnull().sum())

# target
pc_df['pallet_capacity'] = np.where(pc_df.pallet_capacity>target,target,pc_df.pallet_capacity)
 
# making averages for those PMGs which had no sales
pc_df_avg = pc_df.groupby(['pmg'])['pallet_capacity'].mean().reset_index()
pc_df_avg.rename(columns={'pallet_capacity':'pcase_avg_pmg'}, inplace=True)
print(pc_df_avg.loc[pc_df_avg.pmg=='HDL46','pcase_avg_pmg'].sort_values().unique())
pc_df = pc_df[['country', 'tpn', 'supplier_id', 'case_capacity', 'case_type', 'pallet_capacity']] # we do not need pmg later (merging with sales)
print(len(pc_df))

# ### Combining with sales
stores_and_pmg = StoreInputsCreator(path,excel_inputs_f)
stores_and_pmg = stores_and_pmg[['Country','Store','Pmg','Pmg Name','Dep','Division']].drop_duplicates()
stores_and_pmg.columns = stores_and_pmg.columns.str.lower()
isold_file = pd.read_csv(path / 'Model_Datasets/isold_oct_2020.zip', compression='zip')
new_isold_file = isold_file.merge(pc_df, on=['country', 'tpn', 'supplier_id', 'case_capacity'], how='left')
new_isold_file['dep'] = new_isold_file.pmg.str[:3]

# Pallet Capacity part 1
pcase_table = new_isold_file[['country', 'store', 'tpn', 'dep', 'pmg', 'pallet_capacity', 'sold_units']].copy()
pcase_table.dropna(axis=0, how='any', inplace=True)

# Sales Ratio in PMG/Dep level
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

# są pmg gdzie niezależnie od sklepu/kraju nie ma danych dla pallet capacity
print(new_isold_file.loc[new_isold_file.pmg=='HDL44','pallet_capacity'].sort_values().unique())


# Pallet Capacity part 2
parameters = stores_and_pmg.merge(pcase_table_store_pmg, on=['country', 'store', 'pmg'], how='left')
parameters = parameters.merge(pcase_table_store_dep, on=['country', 'store', 'dep'], how='left')
parameters = parameters.merge(pcase_table_pmg, on=['pmg'], how='left')
parameters = parameters.merge(pc_df_avg, on=['pmg'], how='left')

"""
Prioritisation
# Priority 1: 'pcase_spmg' = pallet capacity per store / pmg (sales weighted)
# Priority 4: 'pcase_sdep' = pallet capacity per store / dep (sales weighted)
# Priority 2: 'pcase_pmg' = pallet capacity per store / pmg (sales weighted)
# Priority 3: 'pcase_avg_pmg' = pallet capacity per store / pmg (NO sales weighted!)
"""

parameters['pcase'] = np.where(parameters.pcase_spmg>0,parameters.pcase_spmg,0)
parameters.pcase = np.where(((parameters.pcase==0)&(parameters.pcase_pmg>0)),parameters.pcase_pmg,parameters.pcase)
parameters.pcase = np.where(((parameters.pcase==0)&(parameters.pcase_avg_pmg>0)),parameters.pcase_avg_pmg,parameters.pcase)
parameters.pcase = np.where(((parameters.pcase==0)&(parameters.pcase_sdep>0)),parameters.pcase_sdep,parameters.pcase)
parameters.drop(columns={'pcase_spmg','pcase_sdep','pcase_pmg','pcase_avg_pmg'}, inplace=True)

pallet_capacity_df = pc_df_avg.merge(parameters, on='pmg', how='right')
print(pallet_capacity_df.isnull().sum())

pallet_capacity_df['checking'] = np.where((pallet_capacity_df.pcase_avg_pmg>pallet_capacity_df.pcase), pallet_capacity_df.pcase/pallet_capacity_df.pcase_avg_pmg, pallet_capacity_df.pcase_avg_pmg/pallet_capacity_df.pcase)
total_rows = len(pallet_capacity_df)
weird_rows = len(pallet_capacity_df[pallet_capacity_df.checking<0.2])
print(f'We have {weird_rows/total_rows} weird records')
pallet_capacity_df['Pallet_Capacity'] = np.where(pallet_capacity_df.checking<=0.2, pallet_capacity_df.pcase_avg_pmg, pallet_capacity_df.pcase)
print(pallet_capacity_df.sort_values('pcase', ascending=False).head(20))

pallet_capacity_df = pallet_capacity_df[['country', 'store', 'pmg', 'Pallet_Capacity']]
pallet_capacity_df.to_csv(path / 'Model_Datasets/Pallet_Capacity.csv',index=False)