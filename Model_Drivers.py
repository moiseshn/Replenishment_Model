import pandas as pd
from pathlib import Path
import numpy as np
from datetime import datetime

now = datetime. now()
current_time = now. strftime("%H:%M:%S")
print("Current Time =", current_time)

# =============================================================================
# Comment:
# 1,5 min for opening files. 2,5 min Parameters Repl. 10 sec for Parameters on Produce

# 1. Should I remowe watermelons from produce_df?
# 2. losses file has different separator: "," instead of ";"
# 3. Maybe it would be good to change a name dataset_group into produce_parameters? - Think about it
# 4. Maybe it would be good to keep the same column names in downloaded files from hadoop.Such as 'mbo_bgt_losses' or 'mbo_losses_october'
# =============================================================================

dataset_file = 'Repl_Dataset_What_IF.zip'
losses_file = 'losses_09.09-06.10.zip'

main_folder = Path("C:/D/#PRACA/2020/Inputs/Database/B.Customized_Files/")
final_folder = Path("C:/D/#PRACA/2020/Inputs/Database/C.Final_Model_Data/")
in_progres_folder = Path("C:/D/#PRACA/2020/Inputs/Database/B.Customized_Files_what_if/")

sold_units_days = 28
losses_units_days = 28
RC_Capacity_Ratio = (1+(1-0.62))
backstock_target = 0.4
case_capacity_target = 40
RC_delivery_ratio = 0.23
RC_vs_Pallet_capacity = 0.62
Pre_sort_ratio = 0.06
crates_per_module = 8
crates_per_table = 4
sales_cycle = (0.2, 0.2, 0.2, 0.2, 0.2)
fulfill_target = 0.6

def read_excel_file(place, file_name, sheetname, colnames):
    z = pd.read_excel(place/file_name, sheet_name=sheetname, usecols=colnames)
    return pd.DataFrame(z)

def read_csv_file(place, file_name):
    z = pd.read_csv(place/file_name, sep=";")
    return pd.DataFrame(z)

def read_zip_file(place, file_name):
    z = pd.read_csv(place/file_name, compression='zip')
    return pd.DataFrame(z)

def foil_calculation(a):
    dataset_foil_nsrp.loc[:, a] = dataset_foil_nsrp[a].apply(lambda x: x / 2)

def parameters_calculation_a(a, b):
    repl_parameters[a] = repl_parameters.s_ratio*(repl_parameters[b]/repl_parameters.stock)
    repl_parameters.drop([b],axis=1, inplace=True)

def parameters_calculation_b(a, b):
    repl_parameters[a] = np.where(repl_parameters[b]==1,repl_parameters.s_ratio,0)
    repl_parameters.drop([b],axis=1, inplace=True)
    
def rounding(x):
    Final_Drivers[x] = round(Final_Drivers[x])
    
def replace_equipment_zeros(x):
    Drivers[x].replace(np.nan,0, inplace=True)
    
def round_above_70_per(x):
    Drivers[x] = np.where(Drivers[x].sub(Drivers[x].astype(int)).mul(100).astype(int) >= 70, round(Drivers[x]), Drivers[x])

Model_Dataset = read_zip_file(final_folder, dataset_file)
df = read_excel_file(main_folder, "Dataset_Inputs.xlsx", 'stores_and_pmg', ['store', 'pmg'])
dep_profiles = read_excel_file(main_folder, 'Dataset_Inputs.xlsx', 'Dprofiles', ['store', 'dep', 'Trading Days', 'Fridge Doors',
'Eggs displayed at UHT Milks', 'Advertising Headers', 'Racking', 'Day Fill', 'Cardboard Baller', 'Capping Shelves',
'Lift Allowance', 'Distance: WH to SF', 'Distance: WH to Yard', 'Fridge Door Modules', 'Number of Warehouse Fridges',
'Number of Modules', 'Promo displays', 'Pallets Delivery Ratio', 'Backstock Pallet Ratio', 'Customers'])

pmg_profiles = read_excel_file(main_folder, 'Dataset_Inputs.xlsx', 'Pprofiles', ['country', 'format', 'pmg', 'taggingPerc', 'presortPerc', 'prack'])
store_list = read_excel_file(main_folder, 'Dataset_Inputs.xlsx', 'store_list', ['country', 'store', 'format'])
waste_rtc = pd.read_csv(in_progres_folder / losses_file, sep=',')
waste_rtc.columns = waste_rtc.columns.str.replace('mbo_new_losses.', '')
produce_modules = read_excel_file(main_folder, "Dataset_Inputs.xlsx", 'produce_modules', ['store', 'country', 'sqm', 'tables', 'multidecks'])
produce_df = read_excel_file(main_folder, "Dataset_Inputs.xlsx", 'produce_dataframe', ['pmg', 'Replenishment_Type', 'Crate_Size', 'Module_capacity', 'RC_capacity'])
volumes_table = read_excel_file(in_progres_folder, 'New_Volumes_September.xlsx', 'Sheet1', ['store', 'pmg', 'cases_delivered', 'product_stocked', 'items_sold', 'sales_excl_vat']) # Volumes < -- probably we should keep it above
volumes_table = volumes_table.replace(np.nan, 0)
cases = volumes_table[['store', 'pmg', 'cases_delivered']]
cases = cases[cases.cases_delivered > 0]
items = volumes_table[['store', 'pmg', 'items_sold']]
items = items[items.items_sold > 0]
products = volumes_table[['store', 'pmg', 'product_stocked']]
products = products[products.product_stocked > 0]
sales = volumes_table[['store', 'pmg', 'sales_excl_vat']]
sales = sales[sales.sales_excl_vat > 0]

now = datetime. now()
current_time = now. strftime("%H:%M:%S")
print("Current Time =", current_time)

## Parameters for Grocery / Fresh / GM
dataset = Model_Dataset[Model_Dataset.capacity>0].copy() # temporary. Next time backstock calc based on this but rest of drivers calc based on full table
dataset = dataset.drop(dataset[dataset.pmg=='HDL01'].index) # Remove newspapers from planogram
dataset = dataset.drop_duplicates()
a = dataset[dataset.division != 'Produce'].copy() # Leave just chosen Produce PMGs (planograms)
b = dataset[dataset.division == 'Produce'].copy()
b = b.drop(b[(b.pmg!='PRO16')&(b.pmg!='PRO19')].index)
dataset = pd.concat([a,b])
dataset.stock = np.where(((dataset.stock==0)&(dataset.sold_units>0)),1,dataset.stock) # If we have sales > 0, then stock cannot be equal 0, because: one touch + two touch + capping <> stock
dataset['heavy'] = dataset.weight*dataset.case_capacity # 1. Heavy & Light
dataset.heavy = dataset.heavy.apply(lambda x: 0 if x <= 5 else 1)
dataset.srp = np.where((dataset.srp==1)&(dataset.single_pick=='Y'),0,dataset.srp) # 2. SRP customizing
dataset.nsrp = np.where((dataset.srp==1)&(dataset['pmg'].str.contains("FRZ")),1,dataset.nsrp) 
dataset.srp = np.where((dataset.srp==1)&(dataset['pmg'].str.contains("FRZ")),0,dataset.srp) 
dataset['clipstrip'] = np.where(((dataset.stock-dataset.capacity)/dataset.stock)>0.2,dataset.stock*0.2,dataset.stock-dataset.capacity) # 3. Clipstrips
dataset.clipstrip = np.where((dataset.pmg=="DRY13"),dataset.clipstrip,0) # make variable, just for sweets
dataset.clipstrip = np.where(dataset.clipstrip<0,0,dataset.clipstrip)
dataset['secondary_srp']= np.where(dataset.sold_units/sold_units_days>dataset.capacity, dataset.stock-(dataset.capacity/(1-backstock_target)),0) # 4. Secondary Displays -SRP
dataset.secondary_srp = np.where(((1-dataset.capacity/dataset.stock)>0.4), dataset.secondary_srp,0)
dataset.secondary_srp = np.where(dataset.stock<dataset.capacity,0,dataset.secondary_srp)
dataset.secondary_srp = np.where(dataset.srp==1,dataset.secondary_srp,0)
dataset['secondary_nsrp']= np.where(dataset.sold_units/sold_units_days>dataset.capacity, dataset.stock-(dataset.capacity/(1-backstock_target)),0) # Secondary Displays -NSRP
dataset.secondary_nsrp = np.where((1-(dataset.capacity/dataset.stock)>0.4), dataset.secondary_nsrp,0)
dataset.secondary_nsrp = np.where(dataset.stock<dataset.capacity,0, dataset.secondary_nsrp)
dataset.secondary_nsrp = np.where(dataset.srp==0, dataset.secondary_nsrp,0)

Repl_B = dataset.copy() # 5. Foil replenishment
dataset_no_foil = Repl_B[((Repl_B.foil==0)&(Repl_B.srp==1))|((Repl_B.foil==1)&(Repl_B.srp==1))|((Repl_B.foil==0)&(Repl_B.srp==0))].copy()
dataset_foil_nsrp = Repl_B[(Repl_B.foil==1)&(Repl_B.srp==0)].copy()
driver_list = ['sold_units', 'stock', 'capacity', 'secondary_nsrp', 'secondary_srp', 'clipstrip']

for driver in driver_list:
    foil_calculation(driver)

dataset_foil_srp = dataset_foil_nsrp.copy()
dataset_foil_srp.srp = 1
dataset_foil_srp.nsrp = 0
Repl_dataset = pd.concat([dataset_no_foil, dataset_foil_nsrp, dataset_foil_srp], ignore_index=True)
Repl_dataset['shop_floor_capacity'] = (Repl_dataset.capacity+Repl_dataset.secondary_nsrp+Repl_dataset.secondary_srp+Repl_dataset.clipstrip) # 6. One/Two touch
Repl_dataset['o_touch'] = np.where(Repl_dataset.stock>Repl_dataset.shop_floor_capacity,Repl_dataset.shop_floor_capacity,Repl_dataset.stock)
c_touch = np.where(((0.15*Repl_dataset.shop_floor_capacity)<(Repl_dataset.stock-Repl_dataset.shop_floor_capacity)),0.15*Repl_dataset.capacity,Repl_dataset.stock-Repl_dataset.shop_floor_capacity)
Repl_dataset['c_touch'] = np.where((Repl_dataset.is_capping_shelf=='Y')&(Repl_dataset.stock>Repl_dataset.shop_floor_capacity),c_touch,0)
Repl_dataset['t_touch'] = np.where(Repl_dataset.stock>Repl_dataset.shop_floor_capacity,Repl_dataset.stock-Repl_dataset.c_touch-Repl_dataset.shop_floor_capacity,0)
sales_ratio = Repl_dataset.groupby(['store', 'pmg']).sold_units.sum().to_frame().reset_index() # 7. Weighted average based on sales
sales_ratio = sales_ratio.rename(columns={'sold_units': 'sales_ratio'})
Repl_dataset = Repl_dataset.merge(sales_ratio, on=['store', 'pmg'], how='left')
Repl_dataset['s_ratio'] = Repl_dataset.sold_units / Repl_dataset.sales_ratio
Repl_dataset = Repl_dataset.drop(['sales_ratio'], axis=1)


cols = ['store', 'tpn', 'pmg', 's_ratio', 'stock', 'o_touch', 't_touch', 'c_touch', 'heavy', 'srp', 'nsrp',
'full_pallet', 'mu', 'single_pick', 'secondary_srp', 'secondary_nsrp', 'clipstrip', 'pallet_capacity'] # 8. Calculate parameters part 1
repl_parameters = Repl_dataset[cols].copy()

x = ['Clip_Strip_ratio', 'Sec_NSRP_ratio', 'Sec_SRP_ratio', 'Capping_Shelf_ratio', 'Backstock_ratio', 'One_Touch_ratio']
y = ['clipstrip', 'secondary_nsrp', 'secondary_srp', 'c_touch', 't_touch', 'o_touch']

for i, j in zip(x, y):
    parameters_calculation_a(i, j)

x = ['Heavy_ratio', 'SRP_ratio', 'NSRP_ratio', 'Full_Pallet_ratio', 'MU_ratio', 'Single_Pick_ratio'] # 9. Calculate parameters part 2
y = ['heavy', 'srp', 'nsrp', 'full_pallet', 'mu', 'single_pick']

for i, j in zip(x, y):
    parameters_calculation_b(i, j)

repl_parameters['Pallet_Capacity'] = (repl_parameters.s_ratio * repl_parameters.pallet_capacity)
repl_parameters = repl_parameters.drop(['pallet_capacity'], axis=1)
dataset[(dataset.srp==0)&(dataset.nsrp==0)&(dataset.full_pallet==0)&(dataset.mu==0)&(dataset.foil==0)].pmg.unique() # here we have to get no pmgs <-- put is somwhere on the end of the script
tpn_count = repl_parameters[['store', 'pmg', 'tpn']] # Sumarizing value in PMG level; Add Active Lines; remove unnecesary columns
tpn_count = tpn_count.drop_duplicates()
tpn_count = tpn_count.groupby(['store', 'pmg']).tpn.count().to_frame('Active_Lines').reset_index()
repl_parameters = repl_parameters.drop(['tpn', 's_ratio', 'stock'], axis=1)
final_parameters = repl_parameters.groupby(['store', 'pmg'], as_index=False).sum()
final_parameters = final_parameters.merge(tpn_count, on=['store', 'pmg'], how='left')
icase_table = dataset.loc[(dataset.nsrp==1), ['store', 'tpn', 'pmg', 'case_capacity', 'sold_units']].copy() # Case Capacity <-- target <-- zobacz czy jak dam iloc to zniknie problem
icase_table['case_capacity'] = np.where(icase_table.case_capacity>case_capacity_target,case_capacity_target,icase_table.case_capacity)
icase_table['case_capacity'] = np.where(((icase_table.pmg=='HDL28')&(icase_table.case_capacity<=6)), 30, icase_table.case_capacity) # TEMPORARY: HDL28 in here we can have icase = 1 but it should't be. We had not many lines in here
sales_ratio = icase_table.groupby(['store', 'pmg']).sold_units.sum().to_frame().reset_index()
sales_ratio['store'] = sales_ratio.store.astype(int)
sales_ratio = sales_ratio.rename(columns={'sold_units': 'total_sales'})
icase_table = icase_table.merge(sales_ratio, on=['store', 'pmg'], how='inner')
icase_table['ratio'] = icase_table.sold_units / icase_table.total_sales
icase_table['icase_x'] = icase_table.case_capacity * icase_table.ratio
icase_table = icase_table.groupby(['store', 'pmg']).icase_x.sum().to_frame().reset_index()
icase_table_avg = icase_table.groupby('pmg').icase_x.mean().reset_index()
icase_table_avg = icase_table_avg.rename(columns={'icase_x': 'icase_y'})

final_parameters = final_parameters.merge(icase_table, on=['store', 'pmg'], how='left')
final_parameters = final_parameters.merge(icase_table_avg, on=['pmg'], how='left')
final_parameters = final_parameters.replace(np.nan,0)
final_parameters['Case_Capacity'] = np.where(final_parameters.icase_x==0,final_parameters.icase_y,final_parameters.icase_x)
final_parameters = final_parameters.drop(['icase_x', 'icase_y'], axis=1)

averages_pmg = final_parameters.copy() # Making averages for missing PMGs. Firstly, I do it in pmg level, next in department level as sometimes we do not have even one TPN from PMG which might have some Volumes from Zsolt
averages_pmg = averages_pmg.drop(['store'], axis=1)
averages_pmg = averages_pmg.replace(np.nan,1)
averages_pmg = averages_pmg.groupby(['pmg'], as_index=False).mean()

averages_dep = final_parameters.copy()
averages_dep['dep'] = averages_dep.pmg.astype(str).str[:3]
averages_dep['dep'] = np.where((averages_dep.pmg=='HDL01'),'NEW',averages_dep.dep)
averages_dep = averages_dep.drop(['store'], axis=1)
averages_dep = averages_dep.replace(np.nan,1)
averages_dep = averages_dep.groupby(['dep'], as_index=False).mean()

final_parameters = df.merge(final_parameters, on=['store', 'pmg'], how='left') # combining dataframe with opened stores and every valid PMG + NEWSPAPERS
final_parameters.insert(2, "dep", final_parameters.pmg.astype(str).str[:3], True)
final_parameters['dep'] = np.where((final_parameters.pmg=='HDL01'),'NEW',final_parameters.dep)
final_parameters_A = final_parameters[final_parameters.One_Touch_ratio.notnull()] # NOT empty values:  Create separate dataframes with and without values

no_values_file_A = final_parameters[final_parameters.One_Touch_ratio.isnull()] # Empty values (pmg level): Fill no values with average numbers 
no_values_file_A = pd.DataFrame(no_values_file_A, columns=['store', 'pmg', 'dep'])
no_values_file_A = no_values_file_A.merge(averages_pmg, on=['pmg'], how='left')
final_parameters_B = no_values_file_A[no_values_file_A.One_Touch_ratio.notnull()]

no_values_file_B = no_values_file_A[no_values_file_A.One_Touch_ratio.isnull()] # The rest of empty values (dept level):
no_values_file_B = pd.DataFrame(no_values_file_B, columns=['store', 'pmg', 'dep'])
no_values_file_B = no_values_file_B.merge(averages_dep, on=['dep'], how='left')
final_parameters_C = no_values_file_B[no_values_file_B.One_Touch_ratio.notnull()]
final_parameters_Newspaper = no_values_file_B[no_values_file_B.One_Touch_ratio.isnull()]
final_parameters = pd.concat([final_parameters_A,final_parameters_B,final_parameters_C,final_parameters_Newspaper]).sort_values(['store', 'pmg'])

final_parameters.Pallet_Capacity = np.where(final_parameters.dep=='NEW', 1000, final_parameters.Pallet_Capacity) # Palet Capacity for Newspapers - to avoid empty cells in this department:

## PRODUCE 
td = {'pmg': ['PRO01', 'PRO02', 'PRO03', 'PRO04', 'PRO05', 'PRO06', 'PRO07', 'PRO08', 'PRO09', 'PRO10', 'PRO11', 'PRO12', 'PRO13', 
'PRO14', 'PRO15', 'PRO17', 'PRO18'], 
'to_del': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]}
to_delete = pd.DataFrame(td) # removing PMGs which are not in planogram (in here we should calculate outputs just for PRO16 and PRO19):

final_parameters = final_parameters.merge(to_delete, on='pmg', how='left')
final_parameters = final_parameters.drop(final_parameters[final_parameters['to_del'] == 0].index, axis=0)
final_parameters = final_parameters.drop(['to_del'], axis=1)

final_parameters[final_parameters.duplicated(keep=False)] # <-- we cannot use keep False as we remove all records 

now = datetime. now()
current_time = now. strftime("%H:%M:%S")
print("Current Time =", current_time)

## Parameters for Produce
produce_dataset = Model_Dataset[['country', 'store', 'tpn', 'pmg', 'case_capacity', 'pallet_capacity', 'stock', 'sold_units', 'unit_type', 'weight']].copy()
produce_dataset = produce_dataset[produce_dataset.pmg.str.contains('PRO')]
produce_dataset.drop(Model_Dataset[(Model_Dataset.pmg=='PRO16')|(Model_Dataset.pmg=='PRO19')].index, inplace=True) # removing PMGs which are in planogram (PRO16 and PRO19 <-- these PMGs are calculated in Parameters Customizing, as we have them in the planongram)
RC_table = produce_df[['pmg', 'Replenishment_Type', 'RC_capacity']].copy()
produce_dataset = produce_dataset.merge(produce_df, on='pmg', how='left')

# =============================================================================
# Crates Customizing + Average Items in Case
# - custom crates shows a total amount of LARGE CRATES. So, if we have 4 small crates then we treat them as 2 large
# - daily_crates_on_stock = stock crates + sold crates
# - items in case is necesary for categories replenished as an item 
# =============================================================================

x = produce_dataset.groupby(['store', 'pmg', 'Replenishment_Type', 'unit_type']).sold_units.sum().reset_index()
x = x.rename(columns = {'sold_units': 'total_sales'})
produce_dataset = produce_dataset.merge(x, on=['store', 'pmg', 'Replenishment_Type', 'unit_type'], how='inner')
produce_dataset['crates_on_stock'] = (produce_dataset.stock/produce_dataset.case_capacity)
produce_dataset['custom_sold_crates'] = ((produce_dataset.sold_units/sold_units_days)/produce_dataset.case_capacity)
produce_dataset['custom_sold_crates'] = np.where((produce_dataset.Crate_Size=='Small'), produce_dataset.custom_sold_crates/2,produce_dataset.custom_sold_crates)
produce_dataset['custom_sold_crates'] = np.where((produce_dataset.Crate_Size=='Other'), produce_dataset.custom_sold_crates/4,produce_dataset.custom_sold_crates)
produce_dataset['custom_stock_crates'] = np.where((produce_dataset.Crate_Size=='Small'), produce_dataset.crates_on_stock/2,produce_dataset.crates_on_stock)
produce_dataset['custom_stock_crates'] = np.where((produce_dataset.Crate_Size=='Other'), produce_dataset.custom_stock_crates/4,produce_dataset.custom_stock_crates)
produce_dataset['daily_crates_on_stock'] = produce_dataset.custom_sold_crates + produce_dataset.custom_stock_crates # daily_stock = stock on the end of a day + what they sold this day
produce_dataset['items_in_case'] = (produce_dataset.sold_units / produce_dataset.total_sales) * produce_dataset.case_capacity

# =============================================================================
# Capacity
# - custom_crates_on_stock = total of daily crates on stock (daily sold crates + daily stock)
# - custom_tpn_on_stock = total amount of TPNs per pmg and store
# - multideck_group = in here we check how many crates can be filled on modules. We take into consideration:
#     1. stock ratio per pmg/store
#     2. amount of crates per tpn. If we have more than 4 crates per TPN then we put it to warehouse (we have a place just for 4 tpns per one TPN) 
# - backstock = is calculated based on custom_crates_on_stock. So it is daily sold crates + daily stock
# =============================================================================

custom_crates_on_stock = produce_dataset.groupby(['store', 'pmg', 'Replenishment_Type', 'unit_type']).daily_crates_on_stock.sum().to_frame().reset_index()
custom_tpn_on_stock = produce_dataset.groupby(['store', 'pmg', 'Replenishment_Type', 'unit_type']).tpn.count().to_frame().reset_index()
custom_crates_on_stock = custom_crates_on_stock.merge(custom_tpn_on_stock, on=(['store', 'pmg', 'Replenishment_Type', 'unit_type']), how='left')
custom_crates_on_stock = custom_crates_on_stock.merge(produce_modules, on='store', how='left')
dataset_group = custom_crates_on_stock.copy() # checking if we do not have more than 4 crates in one tpn
dataset_group['crates_per_tpn'] = dataset_group.daily_crates_on_stock / dataset_group.tpn
dataset_group['daily_crates_on_stock_tpn'] = np.where(dataset_group.crates_per_tpn > 4, 4 * dataset_group.tpn, dataset_group.daily_crates_on_stock)
x = dataset_group.groupby(['store', 'Replenishment_Type']).agg({'daily_crates_on_stock_tpn': ('sum')}).reset_index()
x = x.rename(columns={'daily_crates_on_stock_tpn': 'total_daily_crates_on_stock'})
dataset_group = dataset_group.merge(x, on=['store', 'Replenishment_Type'], how='left')
dataset_group['crates_ratio'] = dataset_group.daily_crates_on_stock_tpn / dataset_group.total_daily_crates_on_stock
dataset_group['crates_on_SF'] = np.where(dataset_group.Replenishment_Type == 'Multideck', crates_per_module * dataset_group.multidecks, 0) # dataset_group (banana: 3 crates on hammock + 4 below)
dataset_group['crates_on_SF'] = np.where(dataset_group.Replenishment_Type == 'Produce_table', crates_per_table * dataset_group.tables, dataset_group.crates_on_SF)
dataset_group['crates_on_SF'] = np.where(dataset_group.Replenishment_Type == 'Stand', 50, dataset_group.crates_on_SF)
dataset_group['crates_on_SF'] = np.where(dataset_group.Replenishment_Type == 'Hammok', 7, dataset_group.crates_on_SF)
dataset_group['crates_on_SF'] = np.where(dataset_group.Replenishment_Type == 'Bin', 50, dataset_group.crates_on_SF)
dataset_group['one_touch'] = (dataset_group.crates_ratio * dataset_group.crates_on_SF).astype(float) # calculate shop-floor capacity based on knowledge about total amount of modules
dataset_group['backstock'] = np.where(dataset_group.one_touch <= dataset_group.daily_crates_on_stock, dataset_group.daily_crates_on_stock - dataset_group.one_touch, 0).astype(float)
dataset_group = dataset_group.merge(cases, on=['store', 'pmg'], how='left')
dataset_group = dataset_group.merge(items, on=['store', 'pmg'], how='left')
dataset_group = dataset_group.merge(products, on=['store', 'pmg'], how='left')
dataset_group = dataset_group.merge(sales, on=['store', 'pmg'], how='left')
unit_sold_cases = produce_dataset.groupby(['store', 'pmg', 'Replenishment_Type', 'unit_type']).custom_sold_crates.sum().to_frame().reset_index() # sold crates summary per store / pmg
dataset_group = unit_sold_cases.merge(dataset_group, on=['store', 'pmg', 'Replenishment_Type', 'unit_type'], how='inner')
total_sold_cases = produce_dataset.groupby(['store', 'pmg', 'Replenishment_Type']).custom_sold_crates.sum().to_frame().reset_index()
total_sold_cases = total_sold_cases.rename(columns={'custom_sold_crates': 'total_sold_cases'})
dataset_group = total_sold_cases.merge(dataset_group, on=['store', 'pmg', 'Replenishment_Type'], how='inner')
dataset_group['cases_delivered'] = dataset_group.cases_delivered * (dataset_group.custom_sold_crates / dataset_group.total_sold_cases)
dataset_group['items_sold'] = dataset_group.items_sold * (dataset_group.custom_sold_crates / dataset_group.total_sold_cases)
dataset_group['product_stocked'] = dataset_group.product_stocked * (dataset_group.custom_sold_crates / dataset_group.total_sold_cases)
dataset_group['sales'] = dataset_group.sales_excl_vat * (dataset_group.custom_sold_crates / dataset_group.total_sold_cases)
dataset_group = dataset_group[['store', 'pmg', 'Replenishment_Type', 'unit_type', 'one_touch', 'backstock', 'cases_delivered',
'items_sold', 'product_stocked', 'sales', 'custom_sold_crates']]

dataset_group['shelf_filling'] = dataset_group.one_touch 
dataset_group['crates_to_replenish'] = 0
dataset_group['cycle_1']=np.where((((dataset_group['shelf_filling']-(sales_cycle[0]*dataset_group['custom_sold_crates']))/dataset_group['shelf_filling'])<fulfill_target),1,0)
dataset_group['crates_to_replenish']=np.where(dataset_group['cycle_1']>0, ((dataset_group['crates_to_replenish']+(dataset_group['one_touch']-dataset_group['shelf_filling']))+(dataset_group['custom_sold_crates']*sales_cycle[0])), dataset_group['crates_to_replenish'])
dataset_group['shelf_filling']=np.where(dataset_group['cycle_1']>0, dataset_group['one_touch'], dataset_group['shelf_filling']-(dataset_group['custom_sold_crates']*sales_cycle[0]))
dataset_group['cycle_2']=np.where((((dataset_group['shelf_filling']-(sales_cycle[2]*dataset_group['custom_sold_crates']))/dataset_group['shelf_filling'])<fulfill_target),1,0)
dataset_group['crates_to_replenish']=np.where(dataset_group['cycle_2']>0, ((dataset_group['crates_to_replenish']+(dataset_group['one_touch']-dataset_group['shelf_filling']))+(dataset_group['custom_sold_crates']*sales_cycle[1])), dataset_group['crates_to_replenish'])
dataset_group['shelf_filling']=np.where(dataset_group['cycle_2']>0, dataset_group['one_touch'], dataset_group['shelf_filling']-(dataset_group['custom_sold_crates']*sales_cycle[1]))
dataset_group['cycle_3']=np.where((((dataset_group['shelf_filling']-(sales_cycle[2]*dataset_group['custom_sold_crates']))/dataset_group['shelf_filling'])<fulfill_target),1,0)
dataset_group['crates_to_replenish']=np.where(dataset_group['cycle_3']>0, ((dataset_group['crates_to_replenish']+(dataset_group['one_touch']-dataset_group['shelf_filling']))+(dataset_group['custom_sold_crates']*sales_cycle[2])), dataset_group['crates_to_replenish'])
dataset_group['shelf_filling']=np.where(dataset_group['cycle_3']>0, dataset_group['one_touch'], dataset_group['shelf_filling']-(dataset_group['custom_sold_crates']*sales_cycle[2]))
dataset_group['cycle_4']=np.where((((dataset_group['shelf_filling']-(sales_cycle[3]*dataset_group['custom_sold_crates']))/dataset_group['shelf_filling'])<fulfill_target),1,0)
dataset_group['crates_to_replenish']=np.where(dataset_group['cycle_4']>0, ((dataset_group['crates_to_replenish']+(dataset_group['one_touch']-dataset_group['shelf_filling']))+(dataset_group['custom_sold_crates']*sales_cycle[3])), dataset_group['crates_to_replenish'])
dataset_group['shelf_filling']=np.where(dataset_group['cycle_4']>0, dataset_group['one_touch'], dataset_group['shelf_filling']-(dataset_group['custom_sold_crates']*sales_cycle[3]))
dataset_group['cycle_5']=np.where((((dataset_group['shelf_filling']-(sales_cycle[4]*dataset_group['custom_sold_crates']))/dataset_group['shelf_filling'])<fulfill_target),1,0)
dataset_group['crates_to_replenish']=np.where(dataset_group['cycle_5']>0, (dataset_group['crates_to_replenish']+(dataset_group['one_touch']-dataset_group['shelf_filling'])+(dataset_group['custom_sold_crates']*sales_cycle[4])), dataset_group['crates_to_replenish'])
dataset_group['shelf_filling']=np.where(dataset_group['cycle_5']>0, dataset_group['one_touch'], dataset_group['shelf_filling']-(dataset_group['custom_sold_crates']*sales_cycle[4]))

# =============================================================================
# Weekly drivers calculation
# - backstock_cases_replenished is required just to know how many times I need to move 
# - backstock_rc shows amount of RCs which have to be moved on Shop-Floor (sometimes the same RC needs to be moved more than once). So it is NOT amount of RCs bout amout of stock movements
# =============================================================================

dataset_group = dataset_group.merge(RC_table, on=['pmg', 'Replenishment_Type'], how='left')
dataset_group['one_touch_cases'] = (dataset_group.one_touch/(dataset_group.one_touch+dataset_group.backstock))*dataset_group.cases_delivered
dataset_group['backstock_cases'] = (dataset_group.backstock/(dataset_group.one_touch+dataset_group.backstock))*dataset_group.cases_delivered
dataset_group['backstock_cases_frequency']=dataset_group.cycle_1+dataset_group.cycle_2+dataset_group.cycle_3+dataset_group.cycle_4+dataset_group.cycle_5
dataset_group['backstock_cases_replenished'] = ((dataset_group.crates_to_replenish/dataset_group.backstock)*dataset_group.backstock_cases)
dataset_group['pre_sorted_cases'] = dataset_group.backstock_cases_replenished
dataset_group['pre_sorted_rc'] = dataset_group.pre_sorted_cases/dataset_group.RC_capacity
dataset_group['one_touch_rc'] = dataset_group.cases_delivered/dataset_group.RC_capacity
dataset_group['backstock_rc'] = dataset_group.backstock_cases_replenished/dataset_group.RC_capacity
dataset_group['backstock_rc_incl_frequencies'] = dataset_group.backstock_cases_replenished/dataset_group.RC_capacity*dataset_group.backstock_cases_frequency
dataset_group = dataset_group.replace(np.nan, 0)
x = produce_dataset.groupby(['store', 'pmg', 'Replenishment_Type', 'unit_type']).items_in_case.sum().to_frame().reset_index()
dataset_group = dataset_group.merge(x, on=['store', 'pmg', 'Replenishment_Type', 'unit_type'], how='left')
dataset_group = dataset_group[['store', 'pmg', 'Replenishment_Type', 'unit_type', 'items_in_case', 'cases_delivered',
'items_sold', 'product_stocked', 'sales', 'one_touch_cases', 'backstock_cases',
'pre_sorted_cases', 'pre_sorted_rc' ,'one_touch_rc', 'backstock_rc', 'backstock_rc_incl_frequencies']]
x = produce_dataset.groupby(['store', 'pmg', 'Replenishment_Type', 'unit_type']).tpn.count().to_frame().reset_index()
x = x.rename(columns={'tpn': 'Active Lines'})
dataset_group = dataset_group.merge(x, on=['store', 'pmg', 'Replenishment_Type', 'unit_type'], how='left')
x = produce_dataset[['store', 'tpn', 'pmg', 'case_capacity', 'sold_units', 'unit_type', 'Replenishment_Type', 'weight']].copy()
y = x.groupby(['store', 'pmg', 'unit_type', 'Replenishment_Type']).sold_units.sum().to_frame().reset_index()
y = y.rename(columns={'sold_units': 'total_sales'})
x = x.merge(y, on=['store', 'pmg', 'unit_type', 'Replenishment_Type'], how='inner')
x['sales_ratio'] = x.sold_units / x.total_sales
x['case_weight'] = (x.case_capacity * x.weight)
x['heavy'] = np.where(x.case_weight >= 5 , x.total_sales, 0)
x = x.groupby(['store', 'pmg', 'unit_type', 'Replenishment_Type']).agg({'heavy': 'sum', 'total_sales': 'sum'}).reset_index()
x['heavy_ratio'] = x.heavy / x.total_sales 
x = x[['store', 'pmg', 'unit_type', 'Replenishment_Type', 'heavy_ratio']]
x = x.rename(columns={'heavy_ratio': 'Heavy Crates Ratio'})
x['Light Crates Ratio'] = 1 - x['Heavy Crates Ratio']
dataset_group = dataset_group.merge(x, on=['store', 'pmg', 'Replenishment_Type', 'unit_type'], how='left')
produce_parameters = dataset_group

now = datetime. now()
current_time = now. strftime("%H:%M:%S")
print("Current Time =", current_time)

mstr = Model_Dataset
mstr = mstr[['store', 'tpnb', 'unit_type']].copy()
waste_rtc.amount = waste_rtc.amount.abs()
waste_rtc['dep'] = waste_rtc.pmg.astype(str).str[:3]
waste_rtc['amount'] = waste_rtc.amount/losses_units_days*7
waste_rtc = waste_rtc.sort_values('amount', ascending=False) # removal strange 4 records which amount is much higher than in the other stores
waste_rtc.drop(waste_rtc.index[:4], inplace=True)
waste_rtc = waste_rtc.merge(mstr, on=['store', 'tpnb'], how='left')
a = waste_rtc.groupby(['store', 'code', 'dep', 'unit_type']).amount.sum().reset_index()
b = waste_rtc.groupby(['store', 'code', 'dep','unit_type']).tpnb.count().reset_index()
b = b.rename(columns={'tpnb': 'lines'})
waste_rtc = a.merge(b, on=['store', 'code', 'dep', 'unit_type'], how='inner')

DriversA = final_parameters[['store', 'pmg', 'dep', 'Active_Lines', 'Pallet_Capacity', 'Full_Pallet_ratio', 'Single_Pick_ratio', 'MU_ratio', 'One_Touch_ratio', 'Backstock_ratio', 'Capping_Shelf_ratio', 'Heavy_ratio']].copy()
DriversA = DriversA.merge(cases, on=['store', 'pmg'], how='left')
DriversA = DriversA.merge(products, on=['store', 'pmg'], how='left')
DriversA = DriversA.merge(items, on=['store', 'pmg'], how='left')
DriversA = DriversA.merge(sales, on=['store', 'pmg'], how='left')
DriversA=DriversA.dropna(how='all', axis=0)
a = dep_profiles[['store', 'dep', 'Racking', 'Pallets Delivery Ratio', 'Backstock Pallet Ratio']].copy()
DriversA = DriversA.merge(a, on=['store', 'dep'], how='left')
DriversA = DriversA.merge(store_list, on=('store'), how='inner')
DriversA = DriversA.merge(pmg_profiles, on=['country', 'format', 'pmg'], how='left')
DriversA.prack = np.where(DriversA.Racking == 'Y', DriversA.prack, 0)
DriversA['New Delivery - Rollcages'] = ((DriversA.cases_delivered/DriversA.Pallet_Capacity)*RC_Capacity_Ratio)*(1-DriversA['Pallets Delivery Ratio'])
DriversA['New Delivery - Pallets'] = (DriversA.cases_delivered/DriversA.Pallet_Capacity)*DriversA['Pallets Delivery Ratio']
DriversA['Store Replenished Cases'] = DriversA.cases_delivered-((DriversA.cases_delivered*DriversA.Full_Pallet_ratio)+(DriversA.cases_delivered*DriversA.Single_Pick_ratio)+(DriversA.cases_delivered*DriversA.MU_ratio))
DriversA['Pre-sorted Cases'] = DriversA.presortPerc*DriversA.cases_delivered*DriversA['Pallets Delivery Ratio']
DriversA['Tagged_Items'] = DriversA.taggingPerc*DriversA.items_sold
DriversA['Full Pallet Cases'] = DriversA.cases_delivered*DriversA.Full_Pallet_ratio
DriversA['MU cases'] = DriversA.cases_delivered*DriversA.MU_ratio
DriversA['SP cases'] = DriversA.cases_delivered*DriversA.Single_Pick_ratio
DriversA['SP Items'] = DriversA.items_sold*DriversA.Single_Pick_ratio
DriversA['Racking Pallets'] = DriversA.prack*DriversA['New Delivery - Pallets']
DriversA['Replenished Rollcages'] = DriversA['New Delivery - Rollcages']+((DriversA['Pre-sorted Cases']/DriversA.Pallet_Capacity*DriversA['Pallets Delivery Ratio'])*RC_Capacity_Ratio)
DriversA['Replenished Pallets'] = np.where((DriversA['New Delivery - Pallets']-(DriversA['Pre-sorted Cases']/DriversA.Pallet_Capacity*DriversA['Pallets Delivery Ratio']))<=0,0,DriversA['New Delivery - Pallets']-(DriversA['Pre-sorted Cases']/DriversA.Pallet_Capacity*DriversA['Pallets Delivery Ratio']))
DriversA['Backstock Pallets'] = (DriversA['Store Replenished Cases']*(DriversA.Backstock_ratio+DriversA.Capping_Shelf_ratio)/DriversA.Pallet_Capacity*DriversA['Backstock Pallet Ratio']/0.75)
DriversA['Backstock Rollcages'] = (DriversA['Store Replenished Cases']*(DriversA.Backstock_ratio+DriversA.Capping_Shelf_ratio)/DriversA.Pallet_Capacity*(1-DriversA['Backstock Pallet Ratio'])*RC_Capacity_Ratio/0.75)
DriversA['Pre-sorted Rollcages'] = (DriversA['Pre-sorted Cases']/DriversA.Pallet_Capacity*DriversA['Pallets Delivery Ratio'])*RC_Capacity_Ratio
DriversA['Full Pallet'] = (DriversA['Full Pallet Cases']/DriversA.Pallet_Capacity)
DriversA['MU Pallet'] = (DriversA['MU cases']/DriversA.Pallet_Capacity)
DriversA = DriversA.drop(['Racking', 'country', 'format', 'taggingPerc', 'presortPerc', 'Full_Pallet_ratio', 'Single_Pick_ratio', 'MU_ratio'], axis=1)
DriversB = DriversA[['store', 'pmg', 'Store Replenished Cases']].copy()
a = final_parameters[['store', 'pmg', 'Case_Capacity', 'One_Touch_ratio', 'Backstock_ratio', 'Capping_Shelf_ratio', 'Sec_SRP_ratio', 'Sec_NSRP_ratio', 'Clip_Strip_ratio', 'Heavy_ratio', 'SRP_ratio']].copy()
DriversB = DriversB.merge(a, on=['store', 'pmg'], how='inner')
DriversB['New_SRP_ratio'] = np.where(DriversB.SRP_ratio-DriversB.Sec_SRP_ratio<0,0,DriversB.SRP_ratio-DriversB.Sec_SRP_ratio) # Sec_SRP & Sec_NSRP cannot be higher than SRP & NSRP
DriversB['New_NSRP_ratio'] = np.where((1-DriversB.SRP_ratio)-DriversB.Sec_NSRP_ratio<0,0,(1-DriversB.SRP_ratio)-DriversB.Sec_NSRP_ratio)

DriversB['One Touch Cases'] = DriversB['Store Replenished Cases']*DriversB.One_Touch_ratio
DriversB['Backstock Cases'] = DriversB['Store Replenished Cases']*DriversB.Backstock_ratio
DriversB['Capping Shelf Cases'] = DriversB['Store Replenished Cases']*DriversB.Capping_Shelf_ratio
DriversB['L_SRP'] = DriversB['Store Replenished Cases']*(1-DriversB.Heavy_ratio)*DriversB.New_SRP_ratio*(1-DriversB.Clip_Strip_ratio)
DriversB['H_SRP'] = DriversB['Store Replenished Cases']*DriversB.Heavy_ratio*DriversB.New_SRP_ratio*(1-DriversB.Clip_Strip_ratio)
DriversB['L_NSRP'] = DriversB['Store Replenished Cases']*(1-DriversB.Heavy_ratio)*DriversB.New_NSRP_ratio*(1-DriversB.Clip_Strip_ratio)
DriversB['H_NSRP'] = DriversB['Store Replenished Cases']*DriversB.Heavy_ratio*DriversB.New_NSRP_ratio*(1-DriversB.Clip_Strip_ratio)
DriversB['Sec_SRP_cases'] = np.where(DriversB.New_SRP_ratio==0, DriversB['Store Replenished Cases']*DriversB.SRP_ratio, DriversB['Store Replenished Cases']*DriversB.Sec_SRP_ratio)
DriversB['Sec_NSRP_cases'] = np.where(DriversB.New_NSRP_ratio==0, DriversB['Store Replenished Cases']*(1-DriversB.SRP_ratio), DriversB['Store Replenished Cases']*DriversB.Sec_NSRP_ratio)
DriversB['Clip Strip Cases'] = DriversB.Clip_Strip_ratio*(DriversB['Store Replenished Cases']-(DriversB.Sec_SRP_cases+DriversB.Sec_NSRP_cases))
DriversB['L_NSRP_Items'] = DriversB.L_NSRP * DriversB.Case_Capacity
DriversB['H_NSRP_Items'] = DriversB.H_NSRP * DriversB.Case_Capacity
DriversB['Clip Strip Items'] = DriversB['Clip Strip Cases'] * DriversB.Case_Capacity
DriversB['L_Hook Fill Cases'] = np.where((DriversB.pmg=='DRY13')|(DriversB.pmg=='HDL21')|(DriversB.pmg=='PPD02'),DriversB.L_NSRP,0)
DriversB['H_Hook Fill Cases'] = np.where((DriversB.pmg=='DRY13')|(DriversB.pmg=='HDL21')|(DriversB.pmg=='PPD02'),DriversB.H_NSRP,0)
DriversB['Hook Fill Items'] = np.where((DriversB.pmg=='DRY13')|(DriversB.pmg=='HDL21')|(DriversB.pmg=='PPD02'),(DriversB.L_NSRP_Items+DriversB.H_NSRP_Items),0)
DriversB['L_NSRP'] = np.where((DriversB.pmg=='DRY13')|(DriversB.pmg=='HDL21')|(DriversB.pmg=='PPD02'),0,DriversB.L_NSRP)
DriversB['H_NSRP'] = np.where((DriversB.pmg=='DRY13')|(DriversB.pmg=='HDL21')|(DriversB.pmg=='PPD02'),0,DriversB.H_NSRP)
DriversB['L_NSRP_Items'] = np.where((DriversB.pmg=='DRY13')|(DriversB.pmg=='HDL21')|(DriversB.pmg=='PPD02'),0,DriversB.L_NSRP_Items)
DriversB['H_NSRP_Items'] = np.where((DriversB.pmg=='DRY13')|(DriversB.pmg=='HDL21')|(DriversB.pmg=='PPD02'),0,DriversB.H_NSRP_Items)
DriversB['H_SRP'] = DriversB.H_SRP+DriversB.Sec_SRP_cases 
DriversB['L_NSRP'] = DriversB.L_NSRP+DriversB.Sec_NSRP_cases
DriversB = DriversB.drop(['Sec_SRP_cases', 'Sec_NSRP_cases'], axis=1)
DriversB = DriversB.drop(DriversB.iloc[:,3:14],axis=1)
DriversA = DriversA.drop(DriversA.iloc[:,4:8],axis=1)
DriversA = DriversA.drop(['Pallets Delivery Ratio','Backstock Pallet Ratio','prack','Store Replenished Cases'], axis=1)

## Final Drivers
Drivers = DriversA.merge(DriversB, on=['store', 'pmg'], how='inner')
Drivers['Two Touch Cases'] = (Drivers['Backstock Cases'] + Drivers['Capping Shelf Cases'])
Drivers = Drivers.rename(columns={'cases_delivered': 'Cases Delivered', 'product_stocked': 'Products Stocked', 
'items_sold': 'Items Sold', 'sales_excl_vat': 'sales', 'Active_Lines': 'Active Lines'})

cols_order = ['store', 'pmg', 'dep', 'Active Lines', 'Cases Delivered', 'Products Stocked', 'Items Sold', 'sales',
'Store Replenished Cases', 'One Touch Cases', 'Backstock Cases', 'Capping Shelf Cases', 'Two Touch Cases',
'L_SRP', 'H_SRP', 'L_NSRP', 'L_NSRP_Items', 'H_NSRP', 'H_NSRP_Items',
'Clip Strip Cases', 'Clip Strip Items', 'L_Hook Fill Cases', 'H_Hook Fill Cases', 'Hook Fill Items',
'SP cases', 'SP Items', 'Full Pallet Cases', 'MU cases', 'New Delivery - Rollcages',
'New Delivery - Pallets', 'Pre-sorted Cases', 'Pre-sorted Rollcages', 'Replenished Rollcages',
'Replenished Pallets', 'Backstock Rollcages', 'Backstock Pallets', 'Full Pallet', 'MU Pallet',
'Racking Pallets', 'Tagged_Items']
Drivers = pd.DataFrame(Drivers, columns=cols_order)
Drivers['Cosmetic Tag'] = np.where((Drivers.pmg=='HEA05'), Drivers.Tagged_Items, 0)
Drivers['Bottle Tag'] = np.where(Drivers.dep == 'BWS', Drivers.Tagged_Items, 0)
Drivers['CD Tag'] = np.where(Drivers.pmg == 'HDL33', Drivers.Tagged_Items, 0)
Drivers['Electro Tag'] = np.where((Drivers.pmg=='HDL20') | (Drivers.pmg=='HDL32') | (Drivers.pmg=='HDL34') | (Drivers.pmg=='HDL35') | (Drivers.pmg=='HDL36'), Drivers.Tagged_Items, 0)
Drivers['Soft Tag'] = np.where((Drivers['Cosmetic Tag'] + Drivers['Bottle Tag'] + Drivers['CD Tag'] + Drivers['Electro Tag'])==0, Drivers.Tagged_Items, 0)
Drivers = Drivers.drop(['Tagged_Items'], axis=1)
Drivers['Active Lines'] = np.where(Drivers.dep=='NEW', Drivers['Products Stocked'], Drivers['Active Lines']) # Newspaper customizing
Drivers['Store Replenished Cases'] = np.where(Drivers.dep=='NEW', Drivers['Cases Delivered'], Drivers['Store Replenished Cases'])
Drivers['L_NSRP_Items'] = np.where(Drivers.dep=='NEW', Drivers['Cases Delivered'], Drivers.L_NSRP_Items)
Drivers['One Touch Cases'] = np.where(Drivers.dep=='NEW', Drivers['Cases Delivered'], Drivers['One Touch Cases'])

equipment_list = ['New Delivery - Rollcages', 'New Delivery - Pallets', 'Pre-sorted Rollcages', 'Replenished Rollcages', 'Replenished Pallets',
'Backstock Rollcages', 'Backstock Pallets', 'Full Pallet', 'MU Pallet', 'Racking Pallets']

for equipment in equipment_list:
    replace_equipment_zeros(equipment)

for equipment in equipment_list:
    round_above_70_per(equipment)

Drivers = Drivers.drop(['pmg'], axis=1)
Drivers[Drivers.columns[2:]] = Drivers[Drivers.columns[2:]].astype(float)
Drivers = Drivers.groupby(['store', 'dep'], as_index=False).sum()
Drivers['New Delivery - Rollcages'] = np.where(Drivers.dep=='NEW', 5, Drivers['New Delivery - Rollcages'])
Drivers['Replenished Rollcages'] = np.where(Drivers.dep=='NEW', 5, Drivers['Replenished Rollcages'])

## RTC & Waste
RTC_Waste_table = Drivers[['store', 'dep']]
RTC_Waste_table = RTC_Waste_table.merge(waste_rtc, on=['store', 'dep'], how='left') 
RTC_Waste_table = RTC_Waste_table.replace(np.nan, 0)
RTC_Waste_table.code = RTC_Waste_table.code.astype(int)
RTC_Waste_table['RTC Lines'] = np.where(RTC_Waste_table.code == 1, RTC_Waste_table['lines'], 0)
RTC_Waste_table['Waste Lines'] = np.where(((RTC_Waste_table.code == 3)|(RTC_Waste_table.code == 4)), RTC_Waste_table['lines'], 0)
RTC_Waste_table['Food Donation Lines'] = np.where(RTC_Waste_table.code == 544, RTC_Waste_table['lines'], 0)
RTC_Waste_table['RTC Items'] = np.where((RTC_Waste_table.code == 1), RTC_Waste_table['amount'], 0)
RTC_Waste_table['Waste Items'] = np.where((((RTC_Waste_table.code == 3)|(RTC_Waste_table.code == 4))&(RTC_Waste_table.unit_type != 'KG')&(RTC_Waste_table.dep == 'PRO')), RTC_Waste_table['amount'], 0)
RTC_Waste_table['Waste Items'] = np.where((((RTC_Waste_table.code == 3)|(RTC_Waste_table.code == 4))&(RTC_Waste_table.dep != 'PRO')), RTC_Waste_table['amount'] + RTC_Waste_table['Waste Items'], RTC_Waste_table['Waste Items'])
RTC_Waste_table['Food Donation Items'] = np.where(((RTC_Waste_table.code == 544)&(RTC_Waste_table.unit_type != 'KG')&(RTC_Waste_table.dep == 'PRO')), RTC_Waste_table['amount'], 0)
RTC_Waste_table['Food Donation Items'] = np.where(((RTC_Waste_table.code == 544)&(RTC_Waste_table.dep != 'PRO')), RTC_Waste_table['amount'] + RTC_Waste_table['Food Donation Items'], RTC_Waste_table['Food Donation Items'])
RTC_Waste_table['Waste Bulk (one bag)'] = np.where((((RTC_Waste_table.code == 3)|(RTC_Waste_table.code == 4))&(RTC_Waste_table.unit_type == 'KG')&(RTC_Waste_table.dep == 'PRO')), RTC_Waste_table['amount'] / 0.7, 0)
RTC_Waste_table['Food Donation Bulk (one bag)'] = np.where(((RTC_Waste_table.code == 544)&(RTC_Waste_table.unit_type == 'KG')&(RTC_Waste_table.dep == 'PRO')), RTC_Waste_table['amount'], 0)

RTC_Waste_table = RTC_Waste_table.groupby(['store', 'dep']).agg({'RTC Lines': 'sum', 'Waste Lines': 'sum',
'Food Donation Lines': 'sum', 'RTC Items': 'sum',
'Waste Items': 'sum', 'Food Donation Items': 'sum',
'Waste Bulk (one bag)': 'sum', 'Food Donation Bulk (one bag)': 'sum'}).reset_index()

Drivers = Drivers.merge(RTC_Waste_table, on=['store', 'dep'], how='left')
Drivers = Drivers.fillna(0) # RTC & Waste is 0 on Newspaper. So I changed it to 0

## PRODUCE DRIVERS
produce_driveres = produce_parameters.groupby(['store']).cases_delivered.sum().reset_index()

x = produce_parameters.groupby(['store']).product_stocked.sum().reset_index()
produce_driveres = produce_driveres.merge(x, on='store', how='left')

x = produce_parameters.groupby(['store']).items_sold.sum().reset_index()
produce_driveres = produce_driveres.merge(x, on='store', how='left')

x = produce_parameters.groupby(['store']).sales.sum().reset_index()
produce_driveres = produce_driveres.merge(x, on='store', how='left')
produce_driveres = produce_driveres.rename(columns={'cases_delivered': 'Cases Delivered'})
produce_driveres = produce_driveres.rename(columns={'product_stocked': 'Products Stocked'})
produce_driveres = produce_driveres.rename(columns={'items_sold': 'Items Sold'})
produce_driveres['dep'] = 'PRO'

x = produce_parameters.groupby(['store']).backstock_cases.sum().reset_index()
x = x.rename(columns={'backstock_cases': 'Backstock Cases'})
produce_driveres = produce_driveres.merge(x, on='store', how='left')

x = produce_parameters.groupby(['store']).pre_sorted_cases.sum().reset_index()
x = x.rename(columns={'pre_sorted_cases': 'Pre-sorted Crates'})
produce_driveres = produce_driveres.merge(x, on='store', how='left')

x = produce_parameters.groupby(['store']).pre_sorted_rc.sum().reset_index()
x = x.rename(columns={'pre_sorted_rc': 'Pre-sorted Rollcages'})
produce_driveres = produce_driveres.merge(x, on='store', how='left')

x = produce_parameters[(produce_parameters.Replenishment_Type!='Other')]
x = x.groupby(['store']).one_touch_rc.sum().reset_index()
x['one_touch_rc'] = (x.one_touch_rc * RC_delivery_ratio)
produce_driveres = produce_driveres.merge(x, on='store', how='left')
produce_driveres = produce_driveres.rename(columns={'one_touch_rc': 'New Delivery - Rollcages'})

x = produce_parameters[(produce_parameters.Replenishment_Type!='Other')]
x = x.groupby(['store']).one_touch_rc.sum().reset_index()
x = x.rename(columns={'one_touch_rc': 'New Delivery - Pallets'})
x['New Delivery - Pallets'] = (x['New Delivery - Pallets'] - (x['New Delivery - Pallets'] * RC_delivery_ratio)) * RC_vs_Pallet_capacity
produce_driveres = produce_driveres.merge(x, on='store', how='left')

produce_driveres['Replenished Rollcages'] = produce_driveres['New Delivery - Rollcages'] # Replenished Rollcages and Pallets - it is different than on Repl as we do not pre-sort new delivery on produce (just backstock)
produce_driveres['Replenished Pallets'] = produce_driveres['New Delivery - Pallets']

x = produce_parameters[(produce_parameters.Replenishment_Type!='Other')] # Backstock Rollcages (not all RC but just those which have to be replenished)
x = x.groupby(['store']).backstock_rc.sum().reset_index()
x['backstock_rc'] = (x.backstock_rc * RC_delivery_ratio)
produce_driveres = produce_driveres.merge(x, on='store', how='left')
produce_driveres = produce_driveres.rename(columns={'backstock_rc': 'Backstock Rollcages'})

x = produce_parameters[(produce_parameters.Replenishment_Type!='Other')]
x = x.groupby(['store']).backstock_rc.sum().reset_index()
x = x.rename(columns={'backstock_rc': 'Backstock Pallets'})
x['Backstock Pallets'] = (x['Backstock Pallets'] - (x['Backstock Pallets'] * RC_delivery_ratio)) * RC_vs_Pallet_capacity
produce_driveres = produce_driveres.merge(x, on='store', how='left')

x = produce_parameters[['store', 'pmg', 'cases_delivered', 'Heavy Crates Ratio']].copy()
y = x.groupby(['store', 'pmg']).cases_delivered.sum().to_frame().reset_index()
y = y.rename(columns={'cases_delivered': 'total_cases_delivered'})
x = x.merge(y, on=['store', 'pmg'], how='inner')
x['heavy'] = x.cases_delivered * x['Heavy Crates Ratio']
x = x.groupby(['store']).agg({'cases_delivered': 'sum', 'heavy': 'sum'}).reset_index()
x['Heavy Crates Ratio'] = x.heavy / x.cases_delivered
x = x.replace(np.nan, 0)
x['Light Crates Ratio'] = 1 - x['Heavy Crates Ratio']
x = x[['store', 'Heavy Crates Ratio', 'Light Crates Ratio']]
produce_driveres = produce_driveres.merge(x, on='store', how='left')

x = produce_parameters[((produce_parameters.Replenishment_Type=='Multideck')|(produce_parameters.Replenishment_Type=='Produce_table')|(produce_parameters.Replenishment_Type=='Hammok'))&(produce_parameters.unit_type=='KG')]
x = x.groupby(['store']).cases_delivered.sum().reset_index()
x = x.rename(columns={'cases_delivered': 'Green crates case fill'})
produce_driveres = produce_driveres.merge(x, on='store', how='left')

x = produce_parameters[((produce_parameters.Replenishment_Type=='Multideck')|(produce_parameters.Replenishment_Type=='Produce_table')|(produce_parameters.Replenishment_Type=='Hammok'))&(produce_parameters.unit_type!='KG')]
x = x.groupby(['store']).cases_delivered.sum().reset_index()
x = x.rename(columns={'cases_delivered': 'Green crates unit fill'})
produce_driveres = produce_driveres.merge(x, on='store', how='left')

x = produce_parameters[((produce_parameters.Replenishment_Type=='Multideck')|(produce_parameters.Replenishment_Type=='Produce_table')|(produce_parameters.Replenishment_Type=='Hammok'))&(produce_parameters.unit_type!='KG')].copy()
x['Green crates unit fill items'] = x.items_in_case * x.cases_delivered
x = x.groupby(['store'])['Green crates unit fill items'].sum().reset_index()
produce_driveres = produce_driveres.merge(x, on='store', how='left')

x = produce_parameters[(produce_parameters.pmg=='PRO14')] # Bulk Seeds / Nuts / Dried Fruits - CASE
x = x.groupby(['store']).cases_delivered.sum().reset_index()
x = x.rename(columns={'cases_delivered': 'Bulk Product Cases'})
produce_driveres = produce_driveres.merge(x, on='store', how='left')

x = produce_parameters.groupby(['store'])['Active Lines'].sum().to_frame().reset_index()
produce_driveres = produce_driveres.merge(x, on='store', how='left')

x = produce_parameters[(produce_parameters.Replenishment_Type!='Other')]
x = x.groupby(['store']).backstock_rc_incl_frequencies.sum().reset_index()
produce_driveres = produce_driveres.merge(x, on='store', how='left')
produce_driveres['Backstock_Frequency'] = produce_driveres.backstock_rc_incl_frequencies / produce_driveres['Backstock Rollcages']
produce_driveres = produce_driveres.drop(['backstock_rc_incl_frequencies'], axis=1)
produce_driveres['Empty Rollcages'] = produce_driveres['Backstock Rollcages']
produce_driveres['Empty Pallets'] = produce_driveres['Backstock Pallets']

x = produce_parameters[(produce_parameters.pmg=='PRO13')] # Flower + Garden - Tray fill & Unit Fill (potted plant) - herbs
x = x.groupby(['store']).agg({'cases_delivered': ('sum'), 'items_in_case': ('sum')}).reset_index()
x['Potted Plants Cases'] = x.cases_delivered
x['Potted Plants Items'] = x['Potted Plants Cases'] * x.items_in_case
x = x[['store', 'Potted Plants Cases', 'Potted Plants Items']]
produce_driveres = produce_driveres.merge(x, on='store', how='left')

x = produce_parameters[(produce_parameters.pmg=='PRO01')].copy() # Banana_Cases & Banana shelves cases below hammock & Banana Hammock - BUNCH
x = x.groupby(['store']).agg({'cases_delivered': ('sum'), 'items_in_case': ('sum')}).reset_index()
x['Banana Shelves Cases'] = x.cases_delivered * 0.57
x['Banana Hammock Bunch'] = x.cases_delivered * x.items_in_case
x = x.rename(columns={'cases_delivered': 'Banana Cases'})
x = x[['store', 'Banana Cases', 'Banana Shelves Cases', 'Banana Hammock Bunch']]
produce_driveres = produce_driveres.merge(x, on='store', how='left')

x = produce_parameters[(produce_parameters.pmg=='PRO17')] # Flower + Garden - Cut Flower - CASE= Bucket; Cut Flowers
x = x.groupby(['store']).cases_delivered.sum().reset_index()
x = x.rename(columns={'cases_delivered': 'Cut Flowers'})
produce_driveres = produce_driveres.merge(x, on='store', how='left')

x = produce_parameters[(produce_parameters.pmg=='PRO17')]
x = x.groupby(['store']).one_touch_rc.sum().reset_index()
x['one_touch_rc'] = (x.one_touch_rc * RC_delivery_ratio)
x = x.rename(columns={'one_touch_rc': 'Flowers Rollcages'})
produce_driveres = produce_driveres.merge(x, on='store', how='left')

x = produce_parameters[(produce_parameters.pmg=='PRO17')]
x = x.groupby(['store']).one_touch_rc.sum().reset_index()
x = x.rename(columns={'one_touch_rc': 'Flowers Pallets'})
x['Flowers Pallets'] = (x['Flowers Pallets'] - (x['Flowers Pallets'] * RC_delivery_ratio)) * RC_vs_Pallet_capacity
produce_driveres = produce_driveres.merge(x, on='store', how='left')

## Combine Drivers
Final_Drivers = Drivers.append(produce_driveres, sort=False)
Final_Drivers = Final_Drivers.replace(np.nan, 0)
Final_Drivers = Final_Drivers.groupby(['store','dep'], as_index=False).sum()
Final_Drivers = Final_Drivers.merge(dep_profiles, on=['store', 'dep'], how='inner')

Final_Drivers['Active Lines'] = np.where(Final_Drivers['Active Lines'] > Final_Drivers['Products Stocked'], Final_Drivers['Products Stocked'], Final_Drivers['Active Lines'])
Final_Drivers.drop(Final_Drivers[(Final_Drivers['store'] == 12001) & (Final_Drivers['dep']=='HDL')].index, inplace = True)
Final_Drivers.drop(Final_Drivers[(Final_Drivers['store'] == 22001) & (Final_Drivers['dep']=='HDL')].index, inplace = True)
Final_Drivers['dep'].replace(['HDL'], ['GM'], inplace=True)

Final_Drivers['Light Case Ratio'] = (Final_Drivers['L_SRP'] + Final_Drivers['L_NSRP'] + Final_Drivers['Clip Strip Cases'] + Final_Drivers['L_Hook Fill Cases']) / Final_Drivers['Store Replenished Cases'] # Heavy ratio (%)
Final_Drivers['Heavy Case Ratio'] = (1 - Final_Drivers['Light Case Ratio'])
Final_Drivers['Light Case Ratio'] = np.where((Final_Drivers['dep'] == 'NEW'), 1, Final_Drivers['Light Case Ratio'])
Final_Drivers['Heavy Case Ratio'] = np.where((Final_Drivers['dep'] == 'NEW'), 0, Final_Drivers['Heavy Case Ratio'])
x_light = Final_Drivers.groupby('dep')['Light Case Ratio'].mean().reset_index()
x_heavy = Final_Drivers.groupby('dep')['Heavy Case Ratio'].mean().reset_index()
x = x_light.merge(x_heavy, on='dep', how='inner')
x = x.rename(columns={'Light Case Ratio': 'light', 'Heavy Case Ratio': 'heavy'})
Final_Drivers = Final_Drivers.merge(x, on='dep', how='left')
Final_Drivers['Light Case Ratio'] = np.where((Final_Drivers['Light Case Ratio'].isnull()), Final_Drivers['light'], Final_Drivers['Light Case Ratio'])
Final_Drivers['Heavy Case Ratio'] = np.where((Final_Drivers['Heavy Case Ratio'].isnull()), Final_Drivers['heavy'], Final_Drivers['Heavy Case Ratio'])
Final_Drivers = Final_Drivers.drop(['light', 'heavy'], axis=1)

Final_Drivers.drop(Final_Drivers[(Final_Drivers.store==44001)&(Final_Drivers.dep=='PRO')].index, inplace=True) #44001 is missing so I treat it like 44058
benchmark_values = Final_Drivers[(Final_Drivers.store==44058)&(Final_Drivers.dep=='PRO')].reset_index()
benchmark_values.loc[0, 'store'] = 44001
benchmark_values = benchmark_values.iloc[:,1:]
Final_Drivers = pd.concat([Final_Drivers, benchmark_values], axis=0)

Final_Drivers['Backstock Rollcages'] = np.where(Final_Drivers.dep=='PRO', Final_Drivers['Backstock Rollcages'] * 1.5, Final_Drivers['Backstock Rollcages'] * 1.3) # FullFill Backstock Rollcages 70% Grocery/Fresh/GM & 50% Produce
Final_Drivers['Pre-sorted Rollcages'] = np.where(Final_Drivers.dep=='PRO', Final_Drivers['Pre-sorted Rollcages'] * 1.5, Final_Drivers['Pre-sorted Rollcages'] * 1.3)

rounding_list = ['New Delivery - Rollcages', 'New Delivery - Pallets', 'Pre-sorted Rollcages', 'Replenished Rollcages', 'Replenished Pallets', 'Backstock Rollcages', 'Backstock Pallets', 'Full Pallet', 'MU Pallet', 
'Racking Pallets', 'Empty Rollcages', 'Empty Pallets', 'Flowers Rollcages', 'Flowers Pallets']

for i in rounding_list:
    rounding(i)


Final_Drivers.to_excel('Drivers_09.09-06.10.xlsx', index=False)