import pandas as pd
from pathlib import Path
import Replenishment_Model_Functions as rmf

directory = Path('C:/D/Q32021/')
dir_ = Path('c:/D/ReplModel_2021/Model_Inputs/')

items_file = 'sales_march_2021.csv'
cases_file = 'cases_march_2021.csv'
lines_file = 'range_march_2021.csv'
store_inputs_file = 'Stores_Inputs_2021.xlsx' 
items = pd.read_csv(directory/items_file)
cases_table = pd.read_csv(directory/cases_file)
lines = pd.read_csv(directory/lines_file)

storelist = rmf.StoreInputsCreator(dir_,store_inputs_file)
storelist = storelist[['Store']].drop_duplicates()
storelist.rename(columns=({'Store':'store'}),inplace=True)
amount_of_lines = lines[['store', 'tpn', 'pmg']] # incl CLG and other depts
cases_table = cases_table[['store', 'pmg', 'cases']]

new_volume = items.merge(cases_table, on=['store', 'pmg'], how='outer')
new_volume = new_volume.merge(amount_of_lines, on=['store', 'pmg'], how='outer')
new_volume = new_volume.merge(storelist, on=['store'], how='inner')

print(new_volume.isnull().sum())

new_volume.dropna(subset=['pmg'],inplace=True)
new_volume = new_volume[new_volume.pmg!='UNA01']
new_volume = new_volume.rename(columns={'cases': 'cases_delivered', 'sold_units': 'items_sold',
                                       'tpn': 'product_stocked'})

new_volume['dep'] = new_volume.pmg.str[:3]
new_volume = new_volume.sort_values(['store','pmg'])
new_volume = new_volume.fillna(0)
new_volume.to_excel(directory/'Volumes_P1_2021.xlsx', index=False)