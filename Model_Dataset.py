from datetime import datetime
now = datetime. now()
current_time = now. strftime("%H:%M:%S")
print("Current Time =", current_time)

import pandas as pd
from pathlib import Path
import numpy as np
# =============================================================================
# Comments
# 1. Think how to keep all dataset_inputs in one place (not in excel) <-- if possible
# 2. Download sales_excl_values and keep it in Repl_Dataset ('sl_sms')
# 3. calculate in here weekly items sold and stock - in Parameters_Produce put Variables do_of_days=7 (as there I need daily level data)
# keep here all TPNs from Plano and not. Just create a column with info: plano no/yes
# =============================================================================
planogram_file = 'plano_september_2019.csv'
stock_file = 'stock_09.09-06.10.zip'
items_sold_file = 'isold_09.09-06.10.zip'
ops_dev_file = 'ops_dev_november19.csv'
dataset_inputs = 'Dataset_Inputs.xlsx'

main_folder = Path("C:/D/#PRACA/2020/Inputs/Database/B.Customized_Files/")
in_progres_folder = Path("C:/D/#PRACA/2020/Inputs/Database/B.Customized_Files_what_if/")

backstock_target = 0.4
case_capacity_target = 40

def read_excel_file(place, file_name, sheetname, colnames):
    z = pd.read_excel(place/file_name, sheet_name=sheetname, usecols=colnames)
    return pd.DataFrame(z)

def read_csv_file2(place, file_name):
    z = pd.read_csv(place/file_name, sep=",")
    return pd.DataFrame(z)

def read_csv_file(place, file_name):
    z = pd.read_csv(place/file_name, sep=";")
    return pd.DataFrame(z)

def read_zip_file(place, file_name):
    z = pd.read_csv(place/file_name, compression='zip')
    return pd.DataFrame(z)

store_list = read_excel_file(main_folder, dataset_inputs, 'store_list', ['country', 'store'])
pmg = read_excel_file(main_folder, dataset_inputs, 'pmg_groups', ['pmg', 'pmg name', 'division', 'area'])
pmg = pmg[pmg.area=='Replenishment']
capping = read_excel_file(main_folder, dataset_inputs, 'capping', ['store', 'pmg'])
capping['is_capping_shelf'] = 'Y'
planogram = read_csv_file(main_folder, planogram_file)
planogram = planogram[['store', 'tpnb', 'icase', 'capacity']]
planogram = planogram.drop_duplicates(subset=['tpnb', 'icase', 'store', 'capacity'], keep=False)  # here is a mistake: remove (keep=False)
pdupl = len(planogram[planogram.duplicated(subset=['store', 'tpnb'])])
opsdev = read_csv_file(main_folder, ops_dev_file)
opsdev['srp'] = np.where(((opsdev.srp==0)&(opsdev.nsrp==0)), (opsdev.half_pallet+opsdev.split_pallet), opsdev.srp) # temporary: half_pallet & split_palet == srp
odupl = len(opsdev[opsdev.duplicated(subset=['store', 'tpnb'])])
stock = read_zip_file(in_progres_folder, stock_file)
stock.columns = stock.columns.str.replace('mbo_new_stock.', '') # think how to change the name to variable. Maybe just do "names=colnames"
stock = stock[['store', 'tpn', 'stock']]
sdupl = len(stock[stock.duplicated(subset=['store', 'tpn'])])
isold = read_zip_file(in_progres_folder, items_sold_file)
isold.columns = isold.columns.str.replace('mbo_new_isold.', '') # think how to change the name to variable. Maybe just do "names=colnames"
isold = (isold[isold.sold_units>0])
idupl = len(isold[isold.duplicated(subset=['store', 'tpn'])])
pal_cap_file = read_csv_file2(in_progres_folder, 'pallet_capacity.csv')
pal_cap_file.columns = pal_cap_file.columns.str.replace('mbo_pallet_capacity.', '')
pal_cap_file.rename(columns={'case_size': 'case_capacity'}, inplace=True) # we change the name for comparison between 2 tables
pal_cap_file.dropna(axis=0, how='any', inplace=True) # remove all records with at least one null
pal_cap_file = pal_cap_file[pal_cap_file.carton=='Carton'] # we do not need pallets in here - just cartons
pal_cap_file.sort_values(['country', 'store', 'tpn', 'supplier_id', 'modify_date'], ascending=False, inplace=True)
x = len(pal_cap_file) # sorting values and keep the newest date on top of the table
pal_cap_file.drop_duplicates(subset=['country', 'tpn', 'supplier_id', 'case_capacity'], keep='first', inplace=True)
y = len(pal_cap_file) # if we have duplications keep only the first one (on the top - the newest date)
xy = (1 - y / x)
print("We have removed " +"{:.1%}".format(xy) + " duplications with the oldest modified dates")

pal_cap_file = pal_cap_file[['country', 'tpn', 'supplier_id', 'pallet_capacity', 'case_capacity']] # we will need supplier id in here as well
pal_cap_file[pal_cap_file.duplicated()]

isold = isold.merge(pal_cap_file, on=['country', 'tpn', 'supplier_id', 'case_capacity'], how='left')
isold = isold.merge(pmg, on='pmg', how='inner')

pcase_table = isold[['store', 'tpn', 'pmg', 'pallet_capacity', 'sold_units']].copy() # Pallet Capacity part 1
pcase_table['pcase'] = np.where(((pcase_table.pallet_capacity<5)|(pcase_table.pallet_capacity>=2000)), 0, pcase_table.pallet_capacity)
pcase_table.dropna(axis=0, how='any', inplace=True)
pcase_table.drop(pcase_table[pcase_table.pcase==0].index, inplace=True)
pmg_sales_ratio = pcase_table.groupby(['store', 'pmg']).sold_units.sum().to_frame().reset_index()
pmg_sales_ratio['store'] = pmg_sales_ratio.store.astype(int)
pmg_sales_ratio = pmg_sales_ratio.rename(columns={'sold_units': 'pmg_sales'})
pcase_table = pcase_table.merge(pmg_sales_ratio, on=['store', 'pmg'], how='inner')
pcase_table['pmg_ratio'] = pcase_table.sold_units / pcase_table.pmg_sales
pcase_table['pcase_pmg'] = pcase_table.pallet_capacity * pcase_table.pmg_ratio
pcase_table_pmg = pcase_table.groupby(['store', 'pmg']).pcase_pmg.sum().to_frame().reset_index()

pcase_table = isold[['tpn', 'pmg', 'pallet_capacity', 'sold_units']].copy() # Pallet Capacity part 2
pcase_table['pcase'] = np.where(((pcase_table.pallet_capacity<5)|(pcase_table.pallet_capacity>=2000)), 0, pcase_table.pallet_capacity)
pcase_table.dropna(axis=0, how='any', inplace=True)
pcase_table.drop(pcase_table[pcase_table.pcase==0].index, inplace=True)
pmg2_sales_ratio = pcase_table.groupby(['pmg']).sold_units.sum().to_frame().reset_index()
pmg2_sales_ratio = pmg2_sales_ratio.rename(columns={'sold_units': 'pmg2_sales'})
pcase_table = pcase_table.merge(pmg2_sales_ratio, on=['pmg'], how='inner')
pcase_table['pmg2_ratio'] = pcase_table.sold_units / pcase_table.pmg2_sales
pcase_table['pcase_pmg2'] = pcase_table.pallet_capacity * pcase_table.pmg2_ratio
pcase_table_pmg2 = pcase_table.groupby(['pmg']).pcase_pmg2.sum().to_frame().reset_index()

customized_isold = isold.merge(pcase_table_pmg, on=['store', 'pmg'], how='left') # Pallet Capacity part 3
customized_isold = customized_isold.merge(pcase_table_pmg2, on=['pmg'], how='left')
customized_isold.pallet_capacity.replace(np.nan, 0, inplace=True)
customized_isold.pcase_pmg.replace(np.nan, 0, inplace=True)
customized_isold['pallet_capacity'] = np.where(((customized_isold.pallet_capacity<5)|(customized_isold.pallet_capacity>=2000)), 0, customized_isold.pallet_capacity)
customized_isold['pallet_capacity'] = np.where(customized_isold['pallet_capacity']==0, customized_isold['pcase_pmg'], customized_isold['pallet_capacity'])
customized_isold['pallet_capacity'] = np.where(customized_isold['pallet_capacity']==0, customized_isold['pcase_pmg2'], customized_isold['pallet_capacity'])
customized_isold = customized_isold.drop(['pcase_pmg', 'pcase_pmg2'], axis=1)
cdupl = len(isold[isold.duplicated(subset=['store', 'tpn'])])
customized_isold.supplier_type = customized_isold.supplier_type.apply(lambda x: 'Direct' if x == 'E' else 'Transit')

now = datetime. now()
current_time = now. strftime("%H:%M:%S")
print("Current Time =", current_time)
print("In planogram file we have " + str(pdupl) + " duplicates")
print("In opsdev file we have " + str(odupl) + " duplicates")
print("In stock file we have " + str(sdupl) + " duplicates")
print("In items sold file we have " + str(idupl) + " duplicates")
print("In customized items sold file we have " + str(cdupl) + " duplicates")

dataset = customized_isold.merge(stock, on=['store', 'tpn'], how='left') # Merging
dataset = dataset.merge(opsdev, on=['tpnb', 'store'], how='left')
dataset = pd.DataFrame(dataset, columns = ['store', 'tpn', 'tpnb', 'pmg', 'pmg name', 'division', 'unit_type', 'pallet_capacity', 
'case_capacity', 'weight', 'sold_units', 'sales_excl_vat', 'stock', 'srp', 'nsrp', 'full_pallet', 'mu', 'foil', 'half_pallet', 'split_pallet'])
dataset = dataset.replace(np.nan,0)
dataset['nsrp'] = np.where(((dataset.srp==0)&(dataset.nsrp==0)&(dataset.full_pallet==0)&(dataset.mu==0)), 1, dataset['nsrp'])

Repl_Dataset = dataset.merge(planogram, on=['store', 'tpnb'], how='left') # plano and non plano
Repl_Dataset = Repl_Dataset.merge(store_list, on=['store'], how='inner')
Repl_Dataset = Repl_Dataset.replace(np.nan,0)
Repl_Dataset['pallet_capacity'] = np.where(Repl_Dataset.pallet_capacity==0, 1, Repl_Dataset.pallet_capacity)
Repl_Dataset['icase'] = np.where(Repl_Dataset.icase==0, Repl_Dataset.case_capacity, Repl_Dataset.icase)
Repl_Dataset.drop(['case_capacity'], axis=1, inplace=True)
Repl_Dataset.rename(columns={'icase': 'case_capacity'}, inplace=True)
# =============================================================================
# # Plano / NonPlano ratio
# probka = Repl_Dataset[['country', 'division', 'tpnb', 'capacity']].copy()
# probka['plano'] = np.where(probka['capacity']==0, 0, 1)
# probka.drop(['capacity'], axis=1, inplace=True)

# x = probka.groupby(['country', 'division']).plano.count().reset_index(name='count_total_tpn')
# y = probka.groupby(['country', 'division'])['plano'].apply(lambda x: (x==0).sum()).reset_index(name='count_non_plano_tpn')
# xy = x.merge(y, on=['country', 'division'], how='inner')
# xy['ratio'] = xy.count_non_plano_tpn / xy.count_total_tpn
# xy
# =============================================================================
result = len(Repl_Dataset.drop_duplicates())-len(Repl_Dataset.drop_duplicates(subset=['store', 'tpn'])) # checking do we have more than 1 value for the same store / tpn
if result==0:
    Repl_Dataset = Repl_Dataset.drop_duplicates()
    print('OK')
else:
    print('We have problem. There is:', result, 'records with different values. Check why...')

Repl_Dataset['srp'] = Repl_Dataset['srp'].astype(int) # Integer for OpsDev columns
Repl_Dataset['nsrp'] = Repl_Dataset['nsrp'].astype(int)
Repl_Dataset['full_pallet'] = Repl_Dataset['full_pallet'].astype(int)
Repl_Dataset['mu'] = Repl_Dataset['mu'].astype(int)
Repl_Dataset['foil'] = Repl_Dataset['foil'].astype(int)
Repl_Dataset['pallet_capacity'] = Repl_Dataset['pallet_capacity'].astype(int)
Repl_Dataset['decimal'] = (Repl_Dataset.sold_units.sub(Repl_Dataset.sold_units.astype(int))).mul(1000).astype(int) # Unit type
Repl_Dataset['ut'] = np.where((Repl_Dataset['decimal']>0), 'KG', 'SNGL')
Repl_Dataset['unit_type'] = np.where((Repl_Dataset.pmg.str.contains('PRO')), Repl_Dataset['unit_type'], Repl_Dataset['ut'])
Repl_Dataset.drop(['decimal', 'ut'], axis=1, inplace=True)

tesco_bags=[2005107372973, 2005105784679, 2005105784532, 2005105784044, 2005105784051, 2005100375337,
2005100375338, 2005100375340, 2005103002898]
for bag in tesco_bags: # Remove Tesco Bags
    Repl_Dataset.drop(Repl_Dataset[Repl_Dataset['tpn']==bag].index, inplace=True)

Repl_Dataset = Repl_Dataset.merge(capping, on=['store', 'pmg'], how='left') # Capping Shelves
Repl_Dataset = Repl_Dataset.replace(np.nan,'N')
Repl_Dataset['single_pick'] = 'N' 
Repl_Dataset = pd.DataFrame(Repl_Dataset, columns=['country', 'store', 'tpn', 'tpnb', 'pmg', 'pmg name', 'division', 'unit_type', 
'case_capacity', 'capacity', 'pallet_capacity', 'weight', 'sold_units', 'sales_excl_vat', 'stock', 'srp', 'nsrp', 'full_pallet', 'mu',
'foil', 'half_pallet', 'split_pallet', 'is_capping_shelf', 'single_pick'])

now = datetime. now()
current_time = now. strftime("%H:%M:%S")
print("Current Time =", current_time)

parameters_outputs_name = "Repl_Dataset_What_IF.csv"
Repl_Dataset.to_csv(parameters_outputs_name, index=False)

now = datetime. now()
current_time = now. strftime("%H:%M:%S")
print("Current Time =", current_time)