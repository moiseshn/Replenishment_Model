# -*- coding: utf-8 -*-
"""
Created on Mon Mar 30 09:27:28 2020

@author: mborycki
"""
import pandas as pd
import numpy as np

def StoreInputsCreator(folder):
    xls = pd.ExcelFile(folder / 'Model_Inputs/Stores_Inputs.xlsx')
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
    #store_inputs['dep'] = store_inputs.pmg.str[:3]
    store_inputs['Dep'] = np.where(store_inputs.Pmg=='HDL01', 'NEW', store_inputs.Dep)
    store_inputs = store_inputs.merge(capping_df, on=['Store', 'Pmg'], how='left')
    store_inputs['is_capping_shelf'] = store_inputs['is_capping_shelf'].replace(np.nan,0)
    store_inputs = store_inputs.merge(Dprofiles_df, on=['Store', 'Dep'], how='left')
    store_inputs = store_inputs.merge(Pprofiles_df, on=['Country', 'Format', 'Pmg'], how='left')
    return store_inputs

def VolumesCreator(folder,items_f,cases_f,lines_f,dataset_inputs_f):
    items = pd.read_csv(folder/items_f)
    cases_table = pd.read_csv(folder/cases_f)
    lines = pd.read_csv(folder/lines_f)
    
    storelist = pd.read_csv(folder/dataset_inputs_f)
    storelist = storelist[['Store']].drop_duplicates()
    storelist.rename(columns=({'Store':'store'}),inplace=True)
    ile_linii = lines[['store', 'tpn', 'pmg']] # incl CLG and other depts
    cases_table = cases_table[['store', 'pmg', 'cases']]
    new_volume = items.merge(cases_table, on=['store', 'pmg'], how='outer')
    new_volume = new_volume.merge(ile_linii, on=['store', 'pmg'], how='outer')
    new_volume = new_volume.merge(storelist, on=['store'], how='inner')
    new_volume.dropna(subset=['pmg'],inplace=True)
    new_volume = new_volume[new_volume.pmg!='UNA01']
    
    # new_volume.sold_units = new_volume.sold_units.apply(lambda x: x / 4)
    # new_volume.sales_excl_vat = new_volume.sales_excl_vat.apply(lambda x: x / 4)
    # new_volume.cases = new_volume.cases.apply(lambda x: x / 4)
    new_volume = new_volume.rename(columns={'cases':'cases_delivered','sold_units':'items_sold','tpn':'product_stocked'})
    new_volume['dep'] = new_volume.pmg.str[:3]
    new_volume = new_volume.sort_values(['store','pmg'])
    new_volume = new_volume.fillna(0)
    return new_volume

def ReplDatasetTpn(folder,dataset_inputs_f,planogram_f,stock_f,ops_dev_f,items_sold_f):
    store_inputs = pd.read_csv(folder / dataset_inputs_f)
    store_inputs.columns = map(str.lower, store_inputs.columns)
    store_inputs.rename(columns={'pmg name':'pmg_name'},inplace=True)
    store_inputs = store_inputs[['country', 'store', 'pmg', 'pmg_name', 'division','is_capping_shelf']].drop_duplicates()
    planogram = pd.read_csv(folder / planogram_f, sep=';')
    planogram = planogram[['store', 'tpnb', 'icase', 'capacity']]
    planogram = planogram.drop_duplicates(subset=['tpnb', 'icase', 'store', 'capacity'])
    opsdev = pd.read_csv(folder / ops_dev_f, sep=';')
    opsdev['srp'] = np.where(((opsdev.srp==0)&(opsdev.nsrp==0)), (opsdev.half_pallet+opsdev.split_pallet), opsdev.srp) # temporary: half_pallet & split_palet == srp
    stock = pd.read_csv(folder / stock_f, sep=',')
    stock.columns = stock.columns.str.replace('mbo_daily_stock_october.', '') # think how to change the name to variable. Maybe just do "names=colnames"
    stock = stock[['store', 'tpn', 'stock']]
    isold = pd.read_csv(folder / items_sold_f)
    
    Repl_Dataset = isold.merge(stock, on=['store', 'tpn'], how='left')
    Repl_Dataset = Repl_Dataset.merge(opsdev, on=['tpnb', 'store'], how='left')
    Repl_Dataset = Repl_Dataset.merge(store_inputs, on=['country','store','pmg'], how='inner')
    Repl_Dataset = pd.DataFrame(Repl_Dataset, columns = ['country','store', 'tpn', 'tpnb', 'pmg', 'pmg_name', 'division', 'unit_type', 'is_capping_shelf', 
    'case_capacity', 'weight', 'sold_units', 'sales_excl_vat', 'stock', 'srp', 'nsrp', 'full_pallet', 'mu', 'foil', 'half_pallet', 'split_pallet'])
    Repl_Dataset = Repl_Dataset.replace(np.nan,0)
    Repl_Dataset['nsrp'] = np.where(((Repl_Dataset.srp==0)&(Repl_Dataset.nsrp==0)&(Repl_Dataset.full_pallet==0)&(Repl_Dataset.mu==0)), 1, Repl_Dataset['nsrp'])
    Repl_Dataset = Repl_Dataset.merge(planogram, on=['store', 'tpnb'], how='left') # plano and non plano
    Repl_Dataset = Repl_Dataset.replace(np.nan,0)
    Repl_Dataset['icase'] = np.where(Repl_Dataset.icase==0, Repl_Dataset.case_capacity, Repl_Dataset.icase)
    Repl_Dataset.drop(['case_capacity'], axis=1, inplace=True)
    Repl_Dataset.rename(columns={'icase': 'case_capacity'}, inplace=True)
    
    result = len(Repl_Dataset.drop_duplicates())-len(Repl_Dataset.drop_duplicates(subset=['store', 'tpn'])) # checking do we have more than 1 value for the same store / tpn
    if result==0:
        Repl_Dataset = Repl_Dataset.drop_duplicates()
    else:
        print('We have problem. There is:', result, 'records with different values. Check why...')
    
    Repl_Dataset['srp'] = Repl_Dataset['srp'].astype(int) # Integer for OpsDev columns
    Repl_Dataset['nsrp'] = Repl_Dataset['nsrp'].astype(int)
    Repl_Dataset['full_pallet'] = Repl_Dataset['full_pallet'].astype(int)
    Repl_Dataset['mu'] = Repl_Dataset['mu'].astype(int)
    Repl_Dataset['foil'] = Repl_Dataset['foil'].astype(int)
    # Repl_Dataset['decimal'] = (Repl_Dataset.sold_units.sub(Repl_Dataset.sold_units.astype(int))).mul(1000).astype(int) # Unit type
    # Repl_Dataset['ut'] = np.where((Repl_Dataset['decimal']>0), 'KG', 'SNGL')
    # Repl_Dataset['unit_type'] = np.where((Repl_Dataset.pmg.str.contains('PRO')), Repl_Dataset['unit_type'], Repl_Dataset['ut'])
    # Repl_Dataset.drop(['decimal', 'ut'], axis=1, inplace=True) # change 2
    Repl_Dataset['unit_type'] = np.where(Repl_Dataset.unit_type!='KG','SNGL','KG')
    
    tesco_bags=[2005107372973, 2005105784679, 2005105784532, 2005105784044, 2005105784051, 2005100375337,
    2005100375338, 2005100375340, 2005103002898]
    for bag in tesco_bags: # Remove Tesco Bags
        Repl_Dataset.drop(Repl_Dataset[Repl_Dataset['tpn']==bag].index, inplace=True)
    
    Repl_Dataset['single_pick'] = 0 # change 1 
    Repl_Dataset = pd.DataFrame(Repl_Dataset, columns=['country', 'store', 'tpn', 'tpnb', 'pmg', 'pmg_name', 'division', 'unit_type', 
    'case_capacity', 'capacity', 'weight', 'sold_units', 'sales_excl_vat', 'stock', 'srp', 'nsrp', 'full_pallet', 'mu',
    'foil', 'half_pallet', 'split_pallet', 'is_capping_shelf', 'single_pick'])
    return Repl_Dataset

# Parameters
def FoilCalculation(tbl_name,driver_name):
    tbl_name.loc[:, driver_name] = tbl_name[driver_name].apply(lambda x: x / 2)
def ParamCalc_a(tbl_name, a, b):
    tbl_name[a] = tbl_name.s_ratio*(tbl_name[b]/tbl_name.stock)
    tbl_name.drop([b],axis=1, inplace=True)
def ParamCalc_b(tbl_name,a, b):
    tbl_name[a] = np.where(tbl_name[b]==1,tbl_name.s_ratio,0)
    tbl_name.drop([b],axis=1, inplace=True)

def ReplenishmentParameters(folder,Repl_Dataset,sold_units_days,backstock_target,case_capacity_target,inputs,pallet_capacity_f,volumes_f):
    store_inputs = pd.read_csv(folder / inputs)
    store_inputs = store_inputs.rename(columns={'Store':'store','Pmg':'pmg'}) # temporary
    store_inputs = store_inputs[['store', 'pmg']].drop_duplicates()
    volumes_table = pd.read_excel(folder / volumes_f, sheet_name='Sheet1', usecols=['store', 'pmg', 'cases_delivered', 'product_stocked', 'items_sold', 'sales_excl_vat']) # Volumes < -- probably we should keep it above
    volumes_table = volumes_table.replace(np.nan, 0)
    cases = volumes_table[['store', 'pmg', 'cases_delivered']]
    cases = cases[cases.cases_delivered > 0]
    items = volumes_table[['store', 'pmg', 'items_sold']]
    items = items[items.items_sold > 0]
    products = volumes_table[['store', 'pmg', 'product_stocked']]
    products = products[products.product_stocked > 0]
    sales = volumes_table[['store', 'pmg', 'sales_excl_vat']]
    sales = sales[sales.sales_excl_vat > 0]
    pallet_capacity_df = pd.read_csv(folder / pallet_capacity_f)
    pallet_capacity_df = pallet_capacity_df[['store','pmg','Pallet_Capacity']]
    dataset = Repl_Dataset[Repl_Dataset.capacity>0].copy() # temporary. Next time backstock calc based on this but rest of drivers calc based on full table
    dataset = dataset.drop(dataset[dataset.pmg=='HDL01'].index) # Remove newspapers from planogram
    dataset = dataset.drop_duplicates()
    a = dataset[dataset.division != 'Produce'].copy() # Leave just chosen Produce PMGs (planograms)
    b = dataset[dataset.division == 'Produce'].copy()
    b = b.drop(b[(b.pmg!='PRO16')&(b.pmg!='PRO19')].index)
    dataset = pd.concat([a,b])
    dataset.stock = np.where(((dataset.stock==0)&(dataset.sold_units>0)),1,dataset.stock) # If we have sales > 0, then stock cannot be equal 0, because: one touch + two touch + capping <> stock
    dataset['heavy'] = dataset.weight*dataset.case_capacity # 1. Heavy & Light
    dataset.heavy = dataset.heavy.apply(lambda x: 0 if x <= 5 else 1)
    dataset.srp = np.where((dataset.srp==1)&(dataset.single_pick==1),0,dataset.srp) # 2. SRP customizing
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
        FoilCalculation(dataset_foil_nsrp,driver)
    
    dataset_foil_srp = dataset_foil_nsrp.copy()
    dataset_foil_srp.srp = 1
    dataset_foil_srp.nsrp = 0
    Repl_dataset = pd.concat([dataset_no_foil, dataset_foil_nsrp, dataset_foil_srp], ignore_index=True)
    Repl_dataset['shop_floor_capacity'] = (Repl_dataset.capacity+Repl_dataset.secondary_nsrp+Repl_dataset.secondary_srp+Repl_dataset.clipstrip) # 6. One/Two touch
    Repl_dataset['o_touch'] = np.where(Repl_dataset.stock>Repl_dataset.shop_floor_capacity,Repl_dataset.shop_floor_capacity,Repl_dataset.stock)
    c_touch = np.where(((0.15*Repl_dataset.shop_floor_capacity)<(Repl_dataset.stock-Repl_dataset.shop_floor_capacity)),0.15*Repl_dataset.capacity,Repl_dataset.stock-Repl_dataset.shop_floor_capacity)
    Repl_dataset['c_touch'] = np.where((Repl_dataset.is_capping_shelf==1)&(Repl_dataset.stock>Repl_dataset.shop_floor_capacity),c_touch,0)
    Repl_dataset['t_touch'] = np.where(Repl_dataset.stock>Repl_dataset.shop_floor_capacity,Repl_dataset.stock-Repl_dataset.c_touch-Repl_dataset.shop_floor_capacity,0)
    sales_ratio = Repl_dataset.groupby(['store', 'pmg']).sold_units.sum().to_frame().reset_index() # 7. Weighted average based on sales
    sales_ratio = sales_ratio.rename(columns={'sold_units': 'sales_ratio'})
    Repl_dataset = Repl_dataset.merge(sales_ratio, on=['store', 'pmg'], how='left')
    Repl_dataset['s_ratio'] = Repl_dataset.sold_units / Repl_dataset.sales_ratio
    Repl_dataset = Repl_dataset.drop(['sales_ratio'], axis=1)
    
    cols = ['store', 'tpn', 'pmg', 's_ratio', 'stock', 'o_touch', 't_touch', 'c_touch', 'heavy', 'srp', 'nsrp',
    'full_pallet', 'mu', 'single_pick', 'secondary_srp', 'secondary_nsrp', 'clipstrip'] # 8. Calculate parameters part 1
    repl_parameters = Repl_dataset[cols].copy()
    
    x = ['Clip_Strip_ratio', 'Sec_NSRP_ratio', 'Sec_SRP_ratio', 'Capping_Shelf_ratio', 'Backstock_ratio', 'One_Touch_ratio']
    y = ['clipstrip', 'secondary_nsrp', 'secondary_srp', 'c_touch', 't_touch', 'o_touch']
    
    for i, j in zip(x, y):
        ParamCalc_a(repl_parameters, i, j)
    
    x = ['Heavy_ratio', 'SRP_ratio', 'NSRP_ratio', 'Full_Pallet_ratio', 'MU_ratio', 'Single_Pick_ratio'] # 9. Calculate parameters part 2
    y = ['heavy', 'srp', 'nsrp', 'full_pallet', 'mu', 'single_pick']
    
    for i, j in zip(x, y):
        ParamCalc_b(repl_parameters, i, j)
    
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
    
    final_parameters = store_inputs.merge(final_parameters, on=['store', 'pmg'], how='left') # combining dataframe with opened stores and every valid PMG + NEWSPAPERS
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
    
    final_parameters = final_parameters.merge(pallet_capacity_df, on=['store', 'pmg'], how='left')
    final_parameters.Pallet_Capacity = np.where(final_parameters.dep=='NEW', 1000, final_parameters.Pallet_Capacity) # Palet Capacity for Newspapers - to avoid empty cells in this department:
    
    # removing Produce non-plano PMGs (in here we should calculate outputs just for PRO16 and PRO19):
    td = {'pmg': ['PRO01', 'PRO02', 'PRO03', 'PRO04', 'PRO05', 'PRO06', 'PRO07', 'PRO08', 'PRO09', 'PRO10', 'PRO11', 'PRO12', 'PRO13', 
    'PRO14', 'PRO15', 'PRO17', 'PRO18'], 
    'to_del': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]}
    to_delete = pd.DataFrame(td) 
    
    final_parameters = final_parameters.merge(to_delete, on='pmg', how='left')
    final_parameters = final_parameters.drop(final_parameters[final_parameters['to_del'] == 0].index, axis=0)
    final_parameters = final_parameters.drop(['to_del'], axis=1)
    
    # [final_parameters.duplicated(keep=False)] # <-- we cannot use keep False as we remove all records 
    final_parameters = final_parameters.merge(cases, on=['store', 'pmg'], how='left')
    final_parameters = final_parameters.merge(products, on=['store', 'pmg'], how='left')
    final_parameters = final_parameters.merge(items, on=['store', 'pmg'], how='left')
    final_parameters = final_parameters.merge(sales, on=['store', 'pmg'], how='left')
    final_parameters = final_parameters.dropna(how='all', axis=0)
    final_parameters.rename(columns={'store': 'Store', 'pmg': 'Pmg', 'dep': 'Dep'},inplace=True)
    return final_parameters

def ProduceParameters(folder,Repl_Dataset,inputs,sold_units_days,crates_per_module,crates_per_table,volumes_f,sales_cycle,fulfill_target,pallet_capacity_f):

    volumes_table = pd.read_excel(folder / volumes_f, sheet_name='Sheet1', usecols=['store', 'pmg', 'cases_delivered', 'product_stocked', 'items_sold', 'sales_excl_vat']) # Volumes < -- probably we should keep it above
    volumes_table = volumes_table.replace(np.nan, 0)
    cases = volumes_table[['store', 'pmg', 'cases_delivered']]
    cases = cases[cases.cases_delivered > 0]
    items = volumes_table[['store', 'pmg', 'items_sold']]
    items = items[items.items_sold > 0]
    products = volumes_table[['store', 'pmg', 'product_stocked']]
    products = products[products.product_stocked > 0]
    sales = volumes_table[['store', 'pmg', 'sales_excl_vat']]
    sales = sales[sales.sales_excl_vat > 0]
    pallet_capacity_df = pd.read_csv(folder / pallet_capacity_f)
    pallet_capacity_df = pallet_capacity_df[['store','pmg','Pallet_Capacity']]
    xls = pd.ExcelFile(folder / inputs)
    produce_df = pd.read_excel(xls, 'produce_dataframe')
    produce_df = produce_df.rename(columns={'Pmg':'pmg'}) # temporary
    produce_modules = pd.read_excel(xls, 'produce_modules')
    produce_modules = produce_modules.rename(columns={'Country':'country','Store':'store'}) # temporary
    
    produce_dataset = Repl_Dataset[['country', 'store', 'tpn', 'pmg', 'case_capacity', 'stock', 'sold_units', 'unit_type', 'weight']].copy()
    produce_dataset = produce_dataset[produce_dataset.pmg.str.contains('PRO')]
    produce_dataset.drop(Repl_Dataset[(Repl_Dataset.pmg=='PRO16')|(Repl_Dataset.pmg=='PRO19')].index, inplace=True) # removing PMGs which are in planogram (PRO16 and PRO19 <-- these PMGs are calculated in Parameters Customizing, as we have them in the planongram)
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
    produce_parameters = produce_parameters.merge(pallet_capacity_df, on=['store', 'pmg'], how='left')
    produce_parameters.rename(columns={'store': 'Store', 'pmg': 'Pmg'},inplace=True)
    return produce_parameters

def RtcParameters(folder,Repl_Dataset,losses_f,losses_units_days):
    mstr = Repl_Dataset[['store', 'tpnb', 'unit_type']].copy()
    waste_rtc = pd.read_csv(folder / losses_f, sep=',')
    waste_rtc.columns = waste_rtc.columns.str.replace('mbo_bgt_losses.', '')
    waste_rtc['amount'] = waste_rtc.amount.abs()
    waste_rtc['dep'] = waste_rtc.pmg.astype(str).str[:3]
    waste_rtc['amount'] = waste_rtc.amount/losses_units_days*7
    waste_rtc = waste_rtc.sort_values('amount', ascending=False) # removal strange 4 records which amount is much higher than in the other stores
    waste_rtc.drop(waste_rtc.index[:4], inplace=True)
    waste_rtc = waste_rtc.merge(mstr, on=['store', 'tpnb'], how='left')
    a = waste_rtc.groupby(['store', 'code', 'pmg', 'dep', 'unit_type']).amount.sum().reset_index()
    b = waste_rtc.groupby(['store', 'code', 'pmg', 'dep','unit_type']).tpnb.count().reset_index()
    b = b.rename(columns={'tpnb': 'lines'})
    waste_rtc = a.merge(b, on=['store', 'code', 'pmg', 'dep', 'unit_type'], how='inner')
    waste_rtc['dep'] = np.where(waste_rtc.pmg=='HDL01','NEW',waste_rtc.dep)
    waste_rtc.rename(columns={'store': 'Store', 'pmg':'Pmg', 'dep': 'Dep'},inplace=True)
    return waste_rtc

# Drivers and Hours Calculation
def ReplenishmentDrivers(folder,parameters_df,inputs,RC_Capacity_Ratio):
    store_inputs = pd.read_csv(folder / inputs)
    stores_df = store_inputs[['Country','Store','Format','Store Name','Plan Size']].drop_duplicates()
    dep_df = store_inputs[['Store','Dep','Racking','Pallets Delivery Ratio','Backstock Pallet Ratio']].drop_duplicates()
    pmg_df = store_inputs[['Country','Format','Pmg','taggingPerc','presortPerc','prack','AdditionalMovement','PalletDelivery']].drop_duplicates()
    Drivers = parameters_df.copy()
    Drivers = Drivers.merge(stores_df, on=['Store'], how='inner')
    Drivers = Drivers.merge(pmg_df, on=['Country', 'Format', 'Pmg'], how='left')
    Drivers = Drivers.merge(dep_df, on=['Store','Dep'], how='left')
    Drivers.prack = np.where(Drivers.Racking == 1, Drivers.prack, 0)
    Drivers['New Delivery - Rollcages'] = ((Drivers.cases_delivered/Drivers.Pallet_Capacity)*RC_Capacity_Ratio)*(1-Drivers['Pallets Delivery Ratio'])
    Drivers['New Delivery - Pallets'] = (Drivers.cases_delivered/Drivers.Pallet_Capacity)*Drivers['Pallets Delivery Ratio']
    Drivers['Store Replenished Cases'] = Drivers.cases_delivered-((Drivers.cases_delivered*Drivers.Full_Pallet_ratio)+(Drivers.cases_delivered*Drivers.Single_Pick_ratio)+(Drivers.cases_delivered*Drivers.MU_ratio))
    Drivers['Pre-sorted Cases'] = Drivers.presortPerc*Drivers.cases_delivered*Drivers['Pallets Delivery Ratio']
    Drivers['Tagged_Items'] = Drivers.taggingPerc*Drivers.items_sold
    Drivers['Full Pallet Cases'] = Drivers.cases_delivered*Drivers.Full_Pallet_ratio
    Drivers['MU cases'] = Drivers.cases_delivered*Drivers.MU_ratio
    Drivers['SP cases'] = Drivers.cases_delivered*Drivers.Single_Pick_ratio
    Drivers['SP Items'] = Drivers.items_sold*Drivers.Single_Pick_ratio
    Drivers['Racking Pallets'] = Drivers.prack*Drivers['New Delivery - Pallets']
    Drivers['Replenished Rollcages'] = Drivers['New Delivery - Rollcages']+((Drivers['Pre-sorted Cases']/Drivers.Pallet_Capacity*Drivers['Pallets Delivery Ratio'])*RC_Capacity_Ratio)
    Drivers['Replenished Pallets'] = np.where((Drivers['New Delivery - Pallets']-(Drivers['Pre-sorted Cases']/Drivers.Pallet_Capacity*Drivers['Pallets Delivery Ratio']))<=0,0,Drivers['New Delivery - Pallets']-(Drivers['Pre-sorted Cases']/Drivers.Pallet_Capacity*Drivers['Pallets Delivery Ratio']))
    Drivers['Backstock Pallets'] = (Drivers['Store Replenished Cases']*(Drivers.Backstock_ratio+Drivers.Capping_Shelf_ratio)/Drivers.Pallet_Capacity*Drivers['Backstock Pallet Ratio']/0.75)
    Drivers['Backstock Rollcages'] = (Drivers['Store Replenished Cases']*(Drivers.Backstock_ratio+Drivers.Capping_Shelf_ratio)/Drivers.Pallet_Capacity*(1-Drivers['Backstock Pallet Ratio'])*RC_Capacity_Ratio/0.75)
    Drivers['Pre-sorted Rollcages'] = (Drivers['Pre-sorted Cases']/Drivers.Pallet_Capacity*Drivers['Pallets Delivery Ratio'])*RC_Capacity_Ratio
    Drivers['Full Pallet'] = (Drivers['Full Pallet Cases']/Drivers.Pallet_Capacity)
    Drivers['MU Pallet'] = (Drivers['MU cases']/Drivers.Pallet_Capacity)
    Drivers['New_SRP_ratio'] = np.where(Drivers.SRP_ratio-Drivers.Sec_SRP_ratio<0,0,Drivers.SRP_ratio-Drivers.Sec_SRP_ratio) # Sec_SRP & Sec_NSRP cannot be higher than SRP & NSRP
    Drivers['New_NSRP_ratio'] = np.where((1-Drivers.SRP_ratio)-Drivers.Sec_NSRP_ratio<0,0,(1-Drivers.SRP_ratio)-Drivers.Sec_NSRP_ratio)
    Drivers['One Touch Cases'] = Drivers['Store Replenished Cases']*Drivers.One_Touch_ratio
    Drivers['Backstock Cases'] = Drivers['Store Replenished Cases']*Drivers.Backstock_ratio
    Drivers['Capping Shelf Cases'] = Drivers['Store Replenished Cases']*Drivers.Capping_Shelf_ratio
    Drivers['L_SRP'] = Drivers['Store Replenished Cases']*(1-Drivers.Heavy_ratio)*Drivers.New_SRP_ratio*(1-Drivers.Clip_Strip_ratio)
    Drivers['H_SRP'] = Drivers['Store Replenished Cases']*Drivers.Heavy_ratio*Drivers.New_SRP_ratio*(1-Drivers.Clip_Strip_ratio)
    Drivers['L_NSRP'] = Drivers['Store Replenished Cases']*(1-Drivers.Heavy_ratio)*Drivers.New_NSRP_ratio*(1-Drivers.Clip_Strip_ratio)
    Drivers['H_NSRP'] = Drivers['Store Replenished Cases']*Drivers.Heavy_ratio*Drivers.New_NSRP_ratio*(1-Drivers.Clip_Strip_ratio)
    Drivers['Sec_SRP_cases'] = np.where(Drivers.New_SRP_ratio==0, Drivers['Store Replenished Cases']*Drivers.SRP_ratio, Drivers['Store Replenished Cases']*Drivers.Sec_SRP_ratio)
    Drivers['Sec_NSRP_cases'] = np.where(Drivers.New_NSRP_ratio==0, Drivers['Store Replenished Cases']*(1-Drivers.SRP_ratio), Drivers['Store Replenished Cases']*Drivers.Sec_NSRP_ratio)
    Drivers['Clip Strip Cases'] = Drivers.Clip_Strip_ratio*(Drivers['Store Replenished Cases']-(Drivers.Sec_SRP_cases+Drivers.Sec_NSRP_cases))
    Drivers['L_NSRP_Items'] = Drivers.L_NSRP * Drivers.Case_Capacity
    Drivers['H_NSRP_Items'] = Drivers.H_NSRP * Drivers.Case_Capacity
    Drivers['Clip Strip Items'] = Drivers['Clip Strip Cases'] * Drivers.Case_Capacity
    Drivers['L_Hook Fill Cases'] = np.where((Drivers.Pmg=='DRY13')|(Drivers.Pmg=='HDL21')|(Drivers.Pmg=='PPD02'),Drivers.L_NSRP,0)
    Drivers['H_Hook Fill Cases'] = np.where((Drivers.Pmg=='DRY13')|(Drivers.Pmg=='HDL21')|(Drivers.Pmg=='PPD02'),Drivers.H_NSRP,0)
    Drivers['Hook Fill Items'] = np.where((Drivers.Pmg=='DRY13')|(Drivers.Pmg=='HDL21')|(Drivers.Pmg=='PPD02'),(Drivers.L_NSRP_Items+Drivers.H_NSRP_Items),0)
    Drivers['L_NSRP'] = np.where((Drivers.Pmg=='DRY13')|(Drivers.Pmg=='HDL21')|(Drivers.Pmg=='PPD02'),0,Drivers.L_NSRP)
    Drivers['H_NSRP'] = np.where((Drivers.Pmg=='DRY13')|(Drivers.Pmg=='HDL21')|(Drivers.Pmg=='PPD02'),0,Drivers.H_NSRP)
    Drivers['L_NSRP_Items'] = np.where((Drivers.Pmg=='DRY13')|(Drivers.Pmg=='HDL21')|(Drivers.Pmg=='PPD02'),0,Drivers.L_NSRP_Items)
    Drivers['H_NSRP_Items'] = np.where((Drivers.Pmg=='DRY13')|(Drivers.Pmg=='HDL21')|(Drivers.Pmg=='PPD02'),0,Drivers.H_NSRP_Items)
    Drivers['H_SRP'] = Drivers.H_SRP+Drivers.Sec_SRP_cases 
    Drivers['L_NSRP'] = Drivers.L_NSRP+Drivers.Sec_NSRP_cases
    Drivers['Two Touch Cases'] = (Drivers['Backstock Cases'] + Drivers['Capping Shelf Cases'])
    Drivers['One Touch Cases'] = np.where(Drivers.Dep=='NEW', Drivers['cases_delivered'], Drivers['One Touch Cases'])
    Drivers['Active_Lines'] = np.where(Drivers.Dep=='NEW', Drivers['product_stocked'], Drivers['Active_Lines']) # Newspaper customizing
    Drivers['Store Replenished Cases'] = np.where(Drivers.Dep=='NEW', Drivers['cases_delivered'], Drivers['Store Replenished Cases'])
    Drivers['L_NSRP_Items'] = np.where(Drivers.Dep=='NEW', Drivers['cases_delivered'], Drivers.L_NSRP_Items)
    Drivers['Cosmetic Tag'] = np.where((Drivers.Pmg=='HEA05'), Drivers.Tagged_Items, 0)
    Drivers['Bottle Tag'] = np.where(Drivers.Dep == 'BWS', Drivers.Tagged_Items, 0)
    Drivers['CD Tag'] = np.where(Drivers.Pmg == 'HDL33', Drivers.Tagged_Items, 0)
    Drivers['Electro Tag'] = np.where((Drivers.Pmg=='HDL20') | (Drivers.Pmg=='HDL32') | (Drivers.Pmg=='HDL34') | (Drivers.Pmg=='HDL35') | (Drivers.Pmg=='HDL36'), Drivers.Tagged_Items, 0)
    Drivers['Soft Tag'] = np.where((Drivers['Cosmetic Tag'] + Drivers['Bottle Tag'] + Drivers['CD Tag'] + Drivers['Electro Tag'])==0, Drivers.Tagged_Items, 0)
    Drivers = Drivers.rename(columns={'cases_delivered': 'Cases Delivered', 'product_stocked': 'Products Stocked', 
    'items_sold': 'Items Sold', 'sales_excl_vat': 'sales', 'Active_Lines': 'Active Lines'})
    Drivers = Drivers.drop(['Sec_SRP_cases','Sec_NSRP_cases','Racking','Country','Format','taggingPerc', 
    'presortPerc','Full_Pallet_ratio','Single_Pick_ratio','MU_ratio','Pallets Delivery Ratio', 
    'Backstock Pallet Ratio','prack','Tagged_Items','Store Name','Plan Size',
    'Clip_Strip_ratio','Sec_NSRP_ratio','Sec_SRP_ratio','Capping_Shelf_ratio','Backstock_ratio',
    'One_Touch_ratio','Heavy_ratio','SRP_ratio','NSRP_ratio','Case_Capacity','Pallet_Capacity',
    'New_SRP_ratio','New_NSRP_ratio'], axis=1)
    Drivers = Drivers.groupby(['Store', 'Pmg', 'Dep'], as_index=False).sum()
    Drivers['New Delivery - Rollcages'] = np.where(Drivers.Pmg=='HDL01', 5, Drivers['New Delivery - Rollcages'])
    Drivers['Replenished Rollcages'] = np.where(Drivers.Pmg=='HDL01', 5, Drivers['Replenished Rollcages'])
    return Drivers

def ProduceDrivers(folder,parameters_df,RC_delivery_ratio,RC_vs_Pallet_capacity):
    produce_parameters = parameters_df.copy()
    produce_driveres = produce_parameters.groupby(['Store','Pmg']).cases_delivered.sum().reset_index()

    x = produce_parameters.groupby(['Store','Pmg']).product_stocked.sum().reset_index()
    produce_driveres = produce_driveres.merge(x, on=['Store','Pmg'], how='left')
    
    x = produce_parameters.groupby(['Store','Pmg']).items_sold.sum().reset_index()
    produce_driveres = produce_driveres.merge(x, on=['Store','Pmg'], how='left')
    
    x = produce_parameters.groupby(['Store','Pmg']).sales.sum().reset_index()
    produce_driveres = produce_driveres.merge(x, on=['Store','Pmg'], how='left')
    produce_driveres = produce_driveres.rename(columns={'cases_delivered': 'Cases Delivered'})
    produce_driveres = produce_driveres.rename(columns={'product_stocked': 'Products Stocked'})
    produce_driveres = produce_driveres.rename(columns={'items_sold': 'Items Sold'})
    produce_driveres['Dep'] = 'PRO'
    
    x = produce_parameters.groupby(['Store','Pmg']).backstock_cases.sum().reset_index()
    x = x.rename(columns={'backstock_cases': 'Backstock Cases'})
    produce_driveres = produce_driveres.merge(x, on=['Store','Pmg'], how='left')
    
    x = produce_parameters.groupby(['Store','Pmg']).pre_sorted_cases.sum().reset_index()
    x = x.rename(columns={'pre_sorted_cases': 'Pre-sorted Crates'})
    produce_driveres = produce_driveres.merge(x, on=['Store','Pmg'], how='left')
    
    x = produce_parameters.groupby(['Store','Pmg']).pre_sorted_rc.sum().reset_index()
    x = x.rename(columns={'pre_sorted_rc': 'Pre-sorted Rollcages'})
    produce_driveres = produce_driveres.merge(x, on=['Store','Pmg'], how='left')
    
    x = produce_parameters[(produce_parameters.Replenishment_Type!='Other')]
    x = x.groupby(['Store','Pmg']).one_touch_rc.sum().reset_index()
    x['one_touch_rc'] = (x.one_touch_rc * RC_delivery_ratio)
    produce_driveres = produce_driveres.merge(x, on=['Store','Pmg'], how='left')
    produce_driveres = produce_driveres.rename(columns={'one_touch_rc': 'New Delivery - Rollcages'})
    
    x = produce_parameters[(produce_parameters.Replenishment_Type!='Other')]
    x = x.groupby(['Store','Pmg']).one_touch_rc.sum().reset_index()
    x = x.rename(columns={'one_touch_rc': 'New Delivery - Pallets'})
    x['New Delivery - Pallets'] = (x['New Delivery - Pallets'] - (x['New Delivery - Pallets'] * RC_delivery_ratio)) * RC_vs_Pallet_capacity
    produce_driveres = produce_driveres.merge(x, on=['Store','Pmg'], how='left')
    
    produce_driveres['Replenished Rollcages'] = produce_driveres['New Delivery - Rollcages'] # Replenished Rollcages and Pallets - it is different than on Repl as we do not pre-sort new delivery on produce (just backstock)
    produce_driveres['Replenished Pallets'] = produce_driveres['New Delivery - Pallets']
    
    x = produce_parameters[(produce_parameters.Replenishment_Type!='Other')] # Backstock Rollcages (not all RC but just those which have to be replenished)
    x = x.groupby(['Store','Pmg']).backstock_rc.sum().reset_index()
    x['backstock_rc'] = (x.backstock_rc * RC_delivery_ratio)
    produce_driveres = produce_driveres.merge(x, on=['Store','Pmg'], how='left')
    produce_driveres = produce_driveres.rename(columns={'backstock_rc': 'Backstock Rollcages'})
    
    x = produce_parameters[(produce_parameters.Replenishment_Type!='Other')]
    x = x.groupby(['Store','Pmg']).backstock_rc.sum().reset_index()
    x = x.rename(columns={'backstock_rc': 'Backstock Pallets'})
    x['Backstock Pallets'] = (x['Backstock Pallets'] - (x['Backstock Pallets'] * RC_delivery_ratio)) * RC_vs_Pallet_capacity
    produce_driveres = produce_driveres.merge(x, on=['Store', 'Pmg'], how='left')
    
    x = produce_parameters[((produce_parameters.Replenishment_Type=='Multideck')|(produce_parameters.Replenishment_Type=='Produce_table')|(produce_parameters.Replenishment_Type=='Hammok'))&(produce_parameters.unit_type=='KG')]
    x = x.groupby(['Store', 'Pmg']).cases_delivered.sum().reset_index()
    x = x.rename(columns={'cases_delivered': 'Green crates case fill'})
    produce_driveres = produce_driveres.merge(x, on=['Store', 'Pmg'], how='left')
    
    x = produce_parameters[((produce_parameters.Replenishment_Type=='Multideck')|(produce_parameters.Replenishment_Type=='Produce_table')|(produce_parameters.Replenishment_Type=='Hammok'))&(produce_parameters.unit_type!='KG')]
    x = x.groupby(['Store', 'Pmg']).cases_delivered.sum().reset_index()
    x = x.rename(columns={'cases_delivered': 'Green crates unit fill'})
    produce_driveres = produce_driveres.merge(x, on=['Store', 'Pmg'], how='left')
    
    x = produce_parameters[((produce_parameters.Replenishment_Type=='Multideck')|(produce_parameters.Replenishment_Type=='Produce_table')|(produce_parameters.Replenishment_Type=='Hammok'))&(produce_parameters.unit_type!='KG')].copy()
    x['Green crates unit fill items'] = x.items_in_case * x.cases_delivered
    x = x.groupby(['Store', 'Pmg'])['Green crates unit fill items'].sum().reset_index()
    produce_driveres = produce_driveres.merge(x, on=['Store', 'Pmg'], how='left')
    
    x = produce_parameters[(produce_parameters.Pmg=='PRO14')] # Bulk Seeds / Nuts / Dried Fruits - CASE
    x = x.groupby(['Store', 'Pmg']).cases_delivered.sum().reset_index()
    x = x.rename(columns={'cases_delivered': 'Bulk Product Cases'})
    produce_driveres = produce_driveres.merge(x, on=['Store', 'Pmg'], how='left')
    
    x = produce_parameters.groupby(['Store', 'Pmg'])['Active Lines'].sum().to_frame().reset_index()
    produce_driveres = produce_driveres.merge(x, on=['Store', 'Pmg'], how='left')
    
    x = produce_parameters[(produce_parameters.Replenishment_Type!='Other')]
    x = x.groupby(['Store', 'Pmg']).backstock_rc_incl_frequencies.sum().reset_index()
    produce_driveres = produce_driveres.merge(x, on=['Store', 'Pmg'], how='left')
    produce_driveres['Backstock_Frequency'] = produce_driveres.backstock_rc_incl_frequencies / produce_driveres['Backstock Rollcages']
    produce_driveres = produce_driveres.drop(['backstock_rc_incl_frequencies'], axis=1)
    produce_driveres['Empty Rollcages'] = produce_driveres['Backstock Rollcages'] # we calculate it in wrong way but we did not use it in the model as we calculated it in the model's driver sheet
    produce_driveres['Empty Pallets'] = produce_driveres['Backstock Pallets']
    
    x = produce_parameters[(produce_parameters.Pmg=='PRO13')] # Flower + Garden - Tray fill & Unit Fill (potted plant) - herbs
    x = x.groupby(['Store', 'Pmg']).agg({'cases_delivered': ('sum'), 'items_in_case': ('sum')}).reset_index()
    x['Potted Plants Cases'] = x.cases_delivered
    x['Potted Plants Items'] = x['Potted Plants Cases'] * x.items_in_case
    x = x[['Store', 'Pmg', 'Potted Plants Cases', 'Potted Plants Items']]
    produce_driveres = produce_driveres.merge(x, on=['Store', 'Pmg'], how='left')
    
    x = produce_parameters[(produce_parameters.Pmg=='PRO01')].copy() # Banana_Cases & Banana shelves cases below hammock & Banana Hammock - BUNCH
    x = x.groupby(['Store', 'Pmg']).agg({'cases_delivered': ('sum'), 'items_in_case': ('sum')}).reset_index()
    x['Banana Shelves Cases'] = x.cases_delivered * 0.57
    x['Banana Hammock Bunch'] = x.cases_delivered * x.items_in_case
    x = x.rename(columns={'cases_delivered': 'Banana Cases'})
    x = x[['Store', 'Pmg', 'Banana Cases', 'Banana Shelves Cases', 'Banana Hammock Bunch']]
    produce_driveres = produce_driveres.merge(x, on=['Store', 'Pmg'], how='left')
    
    x = produce_parameters[(produce_parameters.Pmg=='PRO17')] # Flower + Garden - Cut Flower - CASE= Bucket; Cut Flowers
    x = x.groupby(['Store', 'Pmg']).cases_delivered.sum().reset_index()
    x = x.rename(columns={'cases_delivered': 'Cut Flowers'})
    produce_driveres = produce_driveres.merge(x, on=['Store', 'Pmg'], how='left')
    
    x = produce_parameters[(produce_parameters.Pmg=='PRO17')]
    x = x.groupby(['Store', 'Pmg']).one_touch_rc.sum().reset_index()
    x['one_touch_rc'] = (x.one_touch_rc * RC_delivery_ratio)
    x = x.rename(columns={'one_touch_rc': 'Flowers Rollcages'})
    produce_driveres = produce_driveres.merge(x, on=['Store', 'Pmg'], how='left')
    
    x = produce_parameters[(produce_parameters.Pmg=='PRO17')]
    x = x.groupby(['Store', 'Pmg']).one_touch_rc.sum().reset_index()
    x = x.rename(columns={'one_touch_rc': 'Flowers Pallets'})
    x['Flowers Pallets'] = (x['Flowers Pallets'] - (x['Flowers Pallets'] * RC_delivery_ratio)) * RC_vs_Pallet_capacity
    produce_driveres = produce_driveres.merge(x, on=['Store', 'Pmg'], how='left')
    return produce_driveres

def RtcDrivers(folder,rtc_waste_df,inputs):
    store_inputs = pd.read_csv(folder / inputs)
    waste_rtc = rtc_waste_df.copy()
    RTC_Waste_table = store_inputs[['Store', 'Pmg', 'Dep']].drop_duplicates()
    RTC_Waste_table = RTC_Waste_table.merge(waste_rtc, on=['Store', 'Pmg', 'Dep'], how='left') 
    RTC_Waste_table = RTC_Waste_table.replace(np.nan, 0)
    RTC_Waste_table['code'] = RTC_Waste_table.code.astype(int)
    RTC_Waste_table['RTC Lines'] = np.where(RTC_Waste_table.code == 1, RTC_Waste_table['lines'], 0)
    RTC_Waste_table['Waste Lines'] = np.where(((RTC_Waste_table.code == 3)|(RTC_Waste_table.code == 4)), RTC_Waste_table['lines'], 0)
    RTC_Waste_table['Food Donation Lines'] = np.where(RTC_Waste_table.code == 544, RTC_Waste_table['lines'], 0)
    RTC_Waste_table['RTC Items'] = np.where((RTC_Waste_table.code == 1), RTC_Waste_table['amount'], 0)
    RTC_Waste_table['Waste Items'] = np.where((((RTC_Waste_table.code == 3)|(RTC_Waste_table.code == 4))&(RTC_Waste_table.unit_type != 'KG')&(RTC_Waste_table.Dep == 'PRO')), RTC_Waste_table['amount'], 0)
    RTC_Waste_table['Waste Items'] = np.where((((RTC_Waste_table.code == 3)|(RTC_Waste_table.code == 4))&(RTC_Waste_table.Dep != 'PRO')), RTC_Waste_table['amount'] + RTC_Waste_table['Waste Items'], RTC_Waste_table['Waste Items'])
    RTC_Waste_table['Food Donation Items'] = np.where(((RTC_Waste_table.code == 544)&(RTC_Waste_table.unit_type != 'KG')&(RTC_Waste_table.Dep == 'PRO')), RTC_Waste_table['amount'], 0)
    RTC_Waste_table['Food Donation Items'] = np.where(((RTC_Waste_table.code == 544)&(RTC_Waste_table.Dep != 'PRO')), RTC_Waste_table['amount'] + RTC_Waste_table['Food Donation Items'], RTC_Waste_table['Food Donation Items'])
    RTC_Waste_table['Waste Bulk (one bag)'] = np.where((((RTC_Waste_table.code == 3)|(RTC_Waste_table.code == 4))&(RTC_Waste_table.unit_type == 'KG')&(RTC_Waste_table.Dep == 'PRO')), RTC_Waste_table['amount'] / 0.7, 0)
    RTC_Waste_table['Food Donation Bulk (one bag)'] = np.where(((RTC_Waste_table.code == 544)&(RTC_Waste_table.unit_type == 'KG')&(RTC_Waste_table.Dep == 'PRO')), RTC_Waste_table['amount'], 0)
    RTC_Waste_table = RTC_Waste_table.groupby(['Store', 'Pmg', 'Dep']).agg({'RTC Lines': 'sum', 'Waste Lines': 'sum',
    'Food Donation Lines': 'sum', 'RTC Items': 'sum','Waste Items': 'sum', 'Food Donation Items': 'sum','Waste Bulk (one bag)': 'sum', 'Food Donation Bulk (one bag)': 'sum'}).reset_index()    
    return RTC_Waste_table

def FinalizingDrivers(folder,inputs,produce_parameters,repl_drivers,produce_drivers,rtc_drivers):
    produce_parameters = pd.read_csv(folder / produce_parameters)
    store_inputs = pd.read_csv(folder / inputs)
    dep_profiles = store_inputs[['Store','Dep','Division','Trading Days','Fridge Doors','Eggs displayed at UHT Milks',
                             'Advertising Headers','Racking','Day Fill','Cardboard Baller','Capping Shelves',
                             'Lift Allowance','Distance: WH to SF','Distance: WH to Yard','Fridge Door Modules',
                             'Number of Warehouse Fridges','Number of Modules','Promo Displays','Pallets Delivery Ratio',
                             'Backstock Pallet Ratio','Customers', 'Fluctuation %','Steps (gates - work)','End Of Repl Tidy - Cases',
                             'Time for customers','ProductReturns_factor','Online price changes','CAYG Lines','CAYG Units','CAYG Cases',
                             'Trading Days - CAYG','Banana Hammock','Fresh CCLB TPN','Prepicked HL item','Trading Days - Dot Com',
                             'Night Fill','Red Labels','GBP_rates','Multifloor allowance','Pre-sort by other depts',
                             'Stock Movement for Bakery and Counter','Stores without counters','VIP replenishment','Check Fridge Temperature','MULTIFLOOR']].drop_duplicates()
    Final_Drivers = repl_drivers.append(produce_drivers, sort=False)
    Final_Drivers = Final_Drivers.merge(rtc_drivers, on=['Store', 'Pmg', 'Dep'], how='left')
    Final_Drivers = Final_Drivers.fillna(0)
    Final_Drivers['Backstock Rollcages'] = np.where(Final_Drivers.Dep=='PRO',Final_Drivers['Backstock Rollcages']*1.5,Final_Drivers['Backstock Rollcages']*1.3) # Fullfil backstock ratio
    Final_Drivers['Bulk Pallets'] = Final_Drivers['Full Pallet']+Final_Drivers['MU Pallet']
    Final_Drivers["Total RC's and Pallets"] = Final_Drivers['Replenished Rollcages']+Final_Drivers['Replenished Pallets']+Final_Drivers['Backstock Rollcages']+Final_Drivers['Backstock Pallets']+Final_Drivers['Bulk Pallets']
    #multifloor_stores = pd.DataFrame({'Store': [12001,12002,12004,12005,12007,22001], 'MULTIFLOOR': 1}) # in this stores we have MULTIFLOOR
    #Final_Drivers = Final_Drivers.merge(multifloor_stores, on='Store', how='left')
    #Final_Drivers['MULTIFLOOR'] = Final_Drivers.MULTIFLOOR.replace(np.nan,0)
    Final_Drivers['Empty Pallets'] = Final_Drivers['Bulk Pallets']+Final_Drivers['Replenished Pallets']
    Final_Drivers['Empty Rollcages'] = Final_Drivers['Replenished Rollcages']
    Final_Drivers['Total Green Crates'] = Final_Drivers['Green crates case fill']+Final_Drivers['Green crates unit fill']
    Final_Drivers['Add_Walking Backstock Cages'] = Final_Drivers['Backstock Rollcages'] 
    Final_Drivers['Add_Walking Cages'] = Final_Drivers['Replenished Rollcages']
    Final_Drivers['Add_Walking Pallets'] = Final_Drivers['Replenished Pallets']
    
    Final_Drivers = Final_Drivers.groupby(['Store','Dep'], as_index=False).sum()
    mods_no = dep_profiles[['Store', 'Dep', 'Number of Modules','ProductReturns_factor']].drop_duplicates()
    Final_Drivers = Final_Drivers.merge(mods_no, on=['Store', 'Dep'], how='left')
    Final_Drivers['Product Returns'] = np.where(Final_Drivers.ProductReturns_factor>0,Final_Drivers['Items Sold']/Final_Drivers.ProductReturns_factor,0)
    Final_Drivers['Modules to go'] = np.where(Final_Drivers['Number of Modules']>66, 66, Final_Drivers['Number of Modules']) # 11*2*3
    Final_Drivers = Final_Drivers.drop(['Number of Modules','ProductReturns_factor'], axis=1)
    Final_Drivers['Case on Pallet'] = Final_Drivers['Cases Delivered']/(Final_Drivers['New Delivery - Pallets']+Final_Drivers['New Delivery - Rollcages']*0.62)
    Final_Drivers['Case on Pallet'] = Final_Drivers['Case on Pallet'].replace(np.nan,50) # if zero then 50 cases on pallet
    Final_Drivers = Final_Drivers.merge(dep_profiles, on=['Store', 'Dep'], how='left')
    Final_Drivers['Active Lines'] = np.where(Final_Drivers['Active Lines'] > Final_Drivers['Products Stocked'], Final_Drivers['Products Stocked'], Final_Drivers['Active Lines'])
    Final_Drivers.drop(Final_Drivers[(Final_Drivers['Store'] == 12001) & (Final_Drivers['Dep']=='HDL')].index, inplace = True)
    Final_Drivers.drop(Final_Drivers[(Final_Drivers['Store'] == 22001) & (Final_Drivers['Dep']=='HDL')].index, inplace = True)
    #Final_Drivers['Dep'].replace(['HDL'], ['GM'], inplace=True)
    
    #Final_Drivers = Final_Drivers.merge(stores_df, on=['Store'], how='inner')
    Final_Drivers['Light Case Ratio'] = (Final_Drivers['L_SRP'] + Final_Drivers['L_NSRP'] + Final_Drivers['Clip Strip Cases'] + Final_Drivers['L_Hook Fill Cases']) / Final_Drivers['Store Replenished Cases'] # Heavy ratio (%)
    Final_Drivers['Heavy Case Ratio'] = (1 - Final_Drivers['Light Case Ratio'])
    Final_Drivers['Light Case Ratio'] = np.where((Final_Drivers['Dep'] == 'NEW'), 1, Final_Drivers['Light Case Ratio'])
    Final_Drivers['Heavy Case Ratio'] = np.where((Final_Drivers['Dep'] == 'NEW'), 0, Final_Drivers['Heavy Case Ratio'])
    x_light = Final_Drivers.groupby('Dep')['Light Case Ratio'].mean().reset_index()
    x_heavy = Final_Drivers.groupby('Dep')['Heavy Case Ratio'].mean().reset_index()
    x = x_light.merge(x_heavy, on='Dep', how='inner')
    x = x.rename(columns={'Light Case Ratio': 'light', 'Heavy Case Ratio': 'heavy'})
    Final_Drivers = Final_Drivers.merge(x, on='Dep', how='left')
    Final_Drivers['Light Case Ratio'] = np.where((Final_Drivers['Light Case Ratio'].isnull()), Final_Drivers['light'], Final_Drivers['Light Case Ratio'])
    Final_Drivers['Heavy Case Ratio'] = np.where((Final_Drivers['Heavy Case Ratio'].isnull()), Final_Drivers['heavy'], Final_Drivers['Heavy Case Ratio'])
    Final_Drivers = Final_Drivers.drop(['light', 'heavy'], axis=1)
    
    x = produce_parameters[['Store', 'cases_delivered', 'Heavy Crates Ratio']].copy()
    y = x.groupby('Store').cases_delivered.sum().to_frame().reset_index()
    y = y.rename(columns={'cases_delivered': 'total_cases_delivered'})
    x = x.merge(y, on='Store', how='inner')
    x['heavy'] = x.cases_delivered * x['Heavy Crates Ratio']
    x = x.groupby('Store').agg({'cases_delivered': 'sum', 'heavy': 'sum'}).reset_index()
    x['Heavy Crates Ratio'] = x.heavy / x.cases_delivered
    x = x.replace(np.nan, 0)
    x['Light Crates Ratio'] = 1 - x['Heavy Crates Ratio']
    x = x[['Store', 'Heavy Crates Ratio', 'Light Crates Ratio']]
    x['Dep'] = 'PRO'
    Final_Drivers = Final_Drivers.merge(x, on=['Store','Dep'], how='left')
    
    Final_Drivers.drop(Final_Drivers[(Final_Drivers.Store==44001)&(Final_Drivers.Dep=='PRO')].index, inplace=True) #44001 is missing so I treat it like 44058
    benchmark_values = Final_Drivers[(Final_Drivers.Store==44058)&(Final_Drivers.Dep=='PRO')].reset_index()
    benchmark_values.loc[0, 'Store'] = 44001
    benchmark_values = benchmark_values.iloc[:,1:]
    Final_Drivers = pd.concat([Final_Drivers, benchmark_values], axis=0)
    
    Final_Drivers.drop(columns={'Two Touch Cases','Pallets Delivery Ratio','One Touch Cases','ProductReturns_factor',
                                'GBP_rates','Flowers Rollcages','Division','Backstock_Frequency',
                                'Backstock Pallet Ratio','ProductReturns_factor'},inplace=True) #we don't need it in the below pivot table
    Final_Drivers = Final_Drivers.replace(np.nan,0)
    return Final_Drivers

def TimeValues(folder,inputs,most_f,drivers_table):
    store_inputs = pd.read_csv(folder / inputs)
    stores_df = store_inputs[['Country','Store','Format','Store Name','Plan Size']].drop_duplicates()
    storelist_array = stores_df[['Country', 'Store', 'Format']].drop_duplicates().values
    
    most_file = pd.ExcelFile(folder/most_f)
    activity_list = pd.read_excel(most_file,'activities')
    activity_list['Freq_Driver_2'] = activity_list['Freq_Driver_2'].replace(np.nan,0)
    activity_list = activity_list.replace(np.nan,'no_driver')
    activities = activity_list[['Activity_key']].copy()
    activities['Country'] = ''
    activities['Format'] = ''
    activities['Dep'] = ''
    activities['Store'] = 0
    times = pd.read_excel(most_file,'times')
    times_array = activities.values
    departments = pd.DataFrame(['DRY','HEA','BWS','DAI','PPD','FRZ','PRO','HDL','NEW'], columns=['Dep'])
    dep_array = departments.values
    
    df_times = pd.DataFrame(columns=activities.columns)
    result = len(storelist_array) * len(dep_array) * len(times_array)
    df_array = np.empty([result,5], dtype='object') # create an empty array
    counter = 0
    for a in range(len(times_array)):
        for d in range(len(dep_array)):
            for s in range(len(storelist_array)):
                df_array[counter][0] = times_array[a][0] # activity name
                df_array[counter][3] = dep_array[d][0] # department
                df_array[counter][1] = storelist_array[s][0] # country
                df_array[counter][2] = storelist_array[s][2] # format
                df_array[counter][4] = storelist_array[s][1] # store
                counter += 1
    df_times = df_times.append(pd.DataFrame(df_array, columns=df_times.columns))
    df_times = df_times.merge(times, on=['Activity_key','Country','Format', 'Dep'], how='left')
    df_times = df_times.merge(activity_list, on=['Activity_key'], how='left')
    df_times.Store = pd.to_numeric(df_times.Store, errors='coerce')
    drivers_df = drivers_table.melt(id_vars=['Store', 'Dep'], var_name=['drivers']) # Drivers: create drivers_df: here we will keep all drivers for all activities
        
    d_values = [1,2,3,4] # Here we VLOOKUP driver values between df_times and drivers_df. We have 4 drivers
    driver_initial_name = 'drivers'
    value_initial_name = 'value'
    for x in d_values:
        driver_new_name = 'Driver_' + str(x)
        value_new_name = 'Driver_' + str(x) + '_value'
        drivers_df.rename(columns={driver_initial_name: driver_new_name}, inplace=True)
        drivers_df.rename(columns={value_initial_name: value_new_name}, inplace=True)
        df_times = df_times.merge(drivers_df, on=['Store', 'Dep', driver_new_name], how='left')
        df_times[value_new_name] = df_times[value_new_name].replace(np.nan,0) # it seems we need NaN there
        driver_initial_name = driver_new_name
        value_initial_name = value_new_name
    driver_new_name = 'Profile' # Profiles
    value_new_name = 'Profile_value'
    drivers_df.rename(columns={driver_initial_name: driver_new_name}, inplace=True)
    drivers_df.rename(columns={value_initial_name: value_new_name}, inplace=True)
    df_times = df_times.merge(drivers_df, on=['Store', 'Dep', driver_new_name], how='left')
    df_times[value_new_name] = df_times[value_new_name].replace(np.nan,0) # it seems we need NaN there
    drivers_df.rename(columns={driver_new_name: 'drivers'}, inplace=True)
    drivers_df.rename(columns={value_new_name: 'value'}, inplace=True)
    return df_times

def CalcModelHours(calc_hours):
    calc_hours.Driver_3_value = np.where((calc_hours.Driver_3_value==0)&(calc_hours.Driver_3=='no_driver'),1,calc_hours.Driver_3_value) # here we have multiplicators and as we cannot divide by 0, I changed the zeros to 1
    calc_hours.Driver_4_value = np.where((calc_hours.Driver_4_value==0)&(calc_hours.Driver_4=='no_driver'),1,calc_hours.Driver_4_value)
    calc_hours['hours'] = (((calc_hours.Driver_1_value+(calc_hours.Driver_2_value*calc_hours.Freq_Driver_2/100))*calc_hours.Driver_3_value*calc_hours.Driver_4_value)*calc_hours.basic_time/60*calc_hours.freq/100)
    calc_hours['hours'] = np.where((calc_hours.Profile_value==0)&(calc_hours.Profile!='no_driver'), 0, calc_hours['hours'])
    return calc_hours

def HoursCalculation(folder,inputs,df_times,RelaxationAllowance):
    hours_df = df_times.copy()
    hours_df['RA_time'] = np.where(hours_df.RA=='Y',hours_df.basic_time*(RelaxationAllowance/100),0)
    hours_df['basic_time'] = hours_df.basic_time+hours_df.RA_time
    hours_df.drop(columns={'RA_time'}, axis=1, inplace=True)
    hours_df = CalcModelHours(hours_df)
    # Movement without an equipment has to be calculated now (excl Produce because in the model we do not calc it <- revise it later)
    df = hours_df.loc[hours_df.Activity_Group=='Stock Movement',['Store','Dep','Activity_Group','hours']].copy()
    df['add_hours'] = np.where(((df.Activity_Group=='Stock Movement')&(df.Dep!="PRO")), df.hours * 0.2, 0) # for Movement without an equipment
    df = df[['Store','Dep','add_hours']]
    df = df.groupby(['Store','Dep'])['add_hours'].sum().reset_index()
    hours_df = hours_df.merge(df, on=['Store', 'Dep'], how='left')
    hours_df['Driver_1_value'] = np.where(hours_df.Driver_1=='Movement without an equipment', hours_df.add_hours, hours_df['Driver_1_value'])
    hours_df['Driver_2_value'] = np.where(hours_df.Driver_2=='Movement without an equipment', hours_df.add_hours, hours_df['Driver_2_value'])
    hours_df['Driver_3_value'] = np.where(hours_df.Driver_3=='Movement without an equipment', hours_df.add_hours, hours_df['Driver_3_value'])
    hours_df['Driver_4_value'] = np.where(hours_df.Driver_4=='Movement without an equipment', hours_df.add_hours, hours_df['Driver_4_value'])
    hours_df.drop(columns={'add_hours'}, axis=1, inplace=True)
    hours_df = CalcModelHours(hours_df)
    
    # Headcount calculation
    headcount_hrs = hours_df.loc[hours_df.Head==1, ('Store', 'Dep','Head','hours')]
    headcount_hrs = headcount_hrs.groupby(['Store', 'Dep'])['hours'].sum().reset_index()
    headcount_hrs['Headcount'] = np.where((((headcount_hrs.hours/40)-round(headcount_hrs.hours/40))>0.05),np.ceil(headcount_hrs.hours/40),round(headcount_hrs.hours/40))
    headcount_hrs.drop(columns={'hours'}, axis=1, inplace=True)
    hours_df = hours_df.merge(headcount_hrs, on=['Store', 'Dep'], how='left')
    hours_df['Driver_1_value'] = np.where(hours_df.Driver_1=='Headcount', hours_df.Headcount, hours_df['Driver_1_value'])
    hours_df['Driver_2_value'] = np.where(hours_df.Driver_2=='Headcount', hours_df.Headcount, hours_df['Driver_2_value'])
    hours_df['Driver_3_value'] = np.where(hours_df.Driver_3=='Headcount', hours_df.Headcount, hours_df['Driver_3_value'])
    hours_df['Driver_4_value'] = np.where(hours_df.Driver_4=='Headcount', hours_df.Headcount, hours_df['Driver_4_value'])
    hours_df.drop(columns={'Headcount'}, axis=1, inplace=True)
    hours_df = CalcModelHours(hours_df)
    store_inputs = pd.read_csv(folder / inputs)
    dep_profiles = store_inputs[['Store','Dep','Division','Trading Days','Fridge Doors','Eggs displayed at UHT Milks',
                             'Advertising Headers','Racking','Day Fill','Cardboard Baller','Capping Shelves',
                             'Lift Allowance','Distance: WH to SF','Distance: WH to Yard','Fridge Door Modules',
                             'Number of Warehouse Fridges','Number of Modules','Promo Displays','Pallets Delivery Ratio',
                             'Backstock Pallet Ratio','Customers', 'Fluctuation %','Steps (gates - work)','End Of Repl Tidy - Cases',
                             'Time for customers','ProductReturns_factor','Online price changes','CAYG Lines','CAYG Units','CAYG Cases',
                             'Trading Days - CAYG','Banana Hammock','Fresh CCLB TPN','Prepicked HL item','Trading Days - Dot Com',
                             'Night Fill','Red Labels','GBP_rates','Multifloor allowance','Pre-sort by other depts',
                             'Stock Movement for Bakery and Counter','Stores without counters','VIP replenishment','Check Fridge Temperature','MULTIFLOOR']].drop_duplicates()
    division_df=dep_profiles[['Store','Dep','Division','GBP_rates']].drop_duplicates()
    hours_df = hours_df.merge(division_df, on=['Store','Dep'], how='left')
    hours_df['Yearly GPB'] = hours_df.GBP_rates*hours_df.hours*52
    return hours_df

def OutputsComparison(folder,times,current_outputs):    
    new_hrs = times.groupby(['Country','Division']).agg({'hours':'sum','Yearly GPB':'sum'}).reset_index()
    new_hrs.rename(columns={'hours':'New_Hours','Yearly GPB':'New_Yearly_GBP'},inplace=True)
    previous_hrs = pd.read_excel(folder/current_outputs)
    previous_hrs = previous_hrs.groupby(['Country','Division']).agg({'hours':'sum','Yearly GPB':'sum'}).reset_index()
    hrs_comparison = previous_hrs.merge(new_hrs, on=['Country','Division'],how='left')
    hrs_comparison['diff_hours'] = hrs_comparison.New_Hours - hrs_comparison.hours
    hrs_comparison['diff_%'] = ((hrs_comparison.diff_hours / hrs_comparison.hours) * 100)
    pd.options.display.float_format = '{:.1f}'.format
    return print('\nDifferences between the last and current version:\n\n',hrs_comparison[['Country','Division','diff_hours','diff_%']],'\n')

def OperationProductivityBasics(times,drivers):
    sales_df = drivers[['Store','Dep','sales']].copy()
    opb = times.groupby(['Country','Store','Format','Dep','Division','V_F']).agg({'hours':'sum','Yearly GPB':'sum'}).sort_values(['Store','Division']).reset_index()
    opb_fix = opb[opb.V_F=='F'].rename(columns={'hours':'Fix Hours','Yearly GPB':'Yearly_GPB_fix'})
    opb_fix.drop('V_F',axis=1,inplace=True)
    opb_var = opb[opb.V_F=='V'].rename(columns={'hours':'Variable Hours','Yearly GPB':'Yearly_GPB_var'})
    opb_var.drop('V_F',axis=1,inplace=True)
    opb_dep = opb_fix.merge(opb_var,on=['Country','Store','Format','Dep','Division'], how='inner')
    opb_dep['Total Hours'] = opb_dep['Fix Hours'] + opb_dep['Variable Hours']
    opb_dep['Yearly GBP'] = opb_dep.Yearly_GPB_fix + opb_dep.Yearly_GPB_var
    opb_dep.drop(['Yearly_GPB_fix','Yearly_GPB_var'], axis=1, inplace=True)
    opb_dep = opb_dep.merge(sales_df,on=['Store','Dep'],how='inner')
    
    opb_div = opb_dep.groupby(['Country','Store','Format','Division']).agg({'Fix Hours':'sum','Variable Hours':'sum','Total Hours':'sum','Yearly GBP':'sum','sales':'sum'}).reset_index()
    opb_div['Variable Currency'] = opb_div['sales'] / opb_div['Total Hours']
    
    opb_dep.drop('sales', axis=1, inplace=True)
    opb_div.drop('sales', axis=1, inplace=True)
    
    insight = times.groupby(['Country','Store','Format','Division','Activity_Group','Suboperation'])['hours'].sum().reset_index()
    insight = insight[insight.hours>0]
    final_drivers = drivers.melt(id_vars=['Store','Dep'], var_name=['Driver_Names'], value_name='Driver_Values').sort_values(['Store','Dep'])
    final_drivers = final_drivers[final_drivers.Driver_Values>0] # remember that 0 means 'No' in the profiles
    return opb_dep,opb_div,insight,final_drivers

def ReportBi(folder,time_values_new,repl_type_bool,dataset_new):
    def CalcBiInputs(tv_new,tv_actual):
        y = 1
        for x in tv_new,tv_actual:
            time_values = x
            version_name = "New" if y == 1 else "Model"
            ## Model Hours
            df_hours = time_values.copy()
            df_hours = df_hours[['Store', 'Dep', 'Suboperation', 'Activity_Group', 'hours']]
            df_hours.rename(columns={'hours':'Hours'},inplace=True)
            df_hours['Version'] = version_name
            if version_name == 'New':
                df_hours_new = df_hours
            else:
                df_hours_act = df_hours
            
            ## Model Drivers
            df_drivers = time_values[['Store', 'Dep', 'Driver_1', 'Driver_2', 'Driver_3','Driver_4', 'Profile','Driver_1_value', 'Driver_2_value', 'Driver_3_value','Driver_4_value','Profile_value']].drop_duplicates()
            df_drivers_a = df_drivers[['Store', 'Dep', 'Driver_1', 'Driver_2', 'Driver_3','Driver_4', 'Profile']]
            df_drivers_a = df_drivers_a.melt(id_vars=['Store','Dep'], var_name=['Drivers'], value_name='Driver').sort_values(['Store','Dep'])
            df_drivers_b = df_drivers[['Store', 'Dep','Driver_1_value', 'Driver_2_value', 'Driver_3_value','Driver_4_value','Profile_value']]
            df_drivers_b = df_drivers_b.melt(id_vars=['Store','Dep'], var_name=['Drivers'], value_name='Value').sort_values(['Store','Dep'])
            df_drivers_b = df_drivers_b[['Value']]
            df_drivers = pd.concat([df_drivers_a, df_drivers_b], axis=1).drop_duplicates()
            df_drivers = df_drivers[df_drivers.Value>0]
            df_drivers['Version'] = version_name
            if version_name == 'New':
                df_drivers_new = df_drivers
            else:
                df_drivers_act = df_drivers
            y+=1
        return df_hours_new, df_hours_act, df_drivers_new, df_drivers_act
    def CalcReplType(dataset_new):
        dataset_act = pd.read_csv(folder/'Model_Datasets/Repl_Dataset_2019.zip',compression='zip')
        y = 1
        for x in dataset_new,dataset_act:
            dataset = x
            version_name = "New" if y == 1 else "Model"
            repl_type_df = dataset[['country','pmg','pmg_name','sold_units','stock', 'srp', 'nsrp', 'full_pallet', 'mu']].copy()
            repl_type_df = repl_type_df.groupby(['country','pmg','pmg_name','srp', 'nsrp', 'full_pallet', 'mu']).agg({'sold_units':'sum','stock':'sum'}).reset_index()
            repl_type_df['Dep'] = repl_type_df.pmg.str[:3]
            repl_type_df = repl_type_df.melt(id_vars=['country','pmg','pmg_name','sold_units','stock','Dep'],var_name=['Replenishment_Type'])
            repl_type_df = repl_type_df[repl_type_df.value==1].sort_values(['country','pmg','Replenishment_Type'])
            repl_type_df['Replenishment_Type'].replace({'srp':'SRP', 'nsrp':'NSRP', 'full_pallet':'Full Pallet', 'mu':'Full Pallet'},inplace=True)
            repl_type_df.drop(columns={'value'},inplace=True)
            repl_type_df['Version'] = version_name
            if version_name == 'New':
                repl_type_df_new = repl_type_df
            else:
                repl_type_df_act = repl_type_df
            y+=1
        return repl_type_df_new, repl_type_df_act
    
    # Hours and Drivers
    time_values_act = pd.read_csv('C:/D/#PRACA/ReplModel/Model_Outputs/time_value_actual.zip',compression='zip')    
    df_hours_new, df_hours_act, df_drivers_new, df_drivers_act = CalcBiInputs(time_values_new,time_values_act)
    pd.concat([df_hours_act,df_hours_new]).to_csv('Model_Comparison_Hours.csv',index=False)
    pd.concat([df_drivers_act,df_drivers_new]).to_csv('Model_Comparison_Drivers.csv',index=False)
    
    ## Model_Comparison_SortingTable
    df_sort_tbl = time_values_new[['Dep', 'Suboperation', 'Activity_Group', 'Driver_1', 'Driver_2', 'Driver_3','Driver_4', 'Profile']].drop_duplicates()
    df_sort_tbl = df_sort_tbl.melt(id_vars=['Dep', 'Suboperation', 'Activity_Group'], var_name=['no'], value_name='Driver')
    df_sort_tbl.drop(columns={'no'},inplace=True)
    df_sort_tbl = df_sort_tbl.drop_duplicates()
    df_sort_tbl = df_sort_tbl[df_sort_tbl.Driver!='no_driver']
    df_sort_tbl.to_csv('Model_Comparison_SortingTable.csv',index=False)
    
    # Replenishment Types
    if repl_type_bool == True:
        repl_type_df_new, repl_type_df_act = CalcReplType(dataset_new)
        pd.concat([repl_type_df_act,repl_type_df_new]).to_csv('Replenishment_Type_summary.csv',index=False)