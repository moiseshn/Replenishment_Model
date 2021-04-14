import pandas as pd
import numpy as np
from pathlib import Path

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


case_capacity_target = 40
df = pd.read_csv('C:/D/#PRACA/ReplModel_2021/Model_Datasets/Repl_Dataset_P8_2020.zip',compression='zip')
directory_new = Path("C:/D/#PRACA/ReplModel_2021/")
excel_inputs_f = 'C:/D/#PRACA/ReplModel_2021/Model_Inputs/Stores_Inputs_2021.xlsx' 

store_inputs = StoreInputsCreator(directory_new,excel_inputs_f)
store_inputs = store_inputs[['Store','Pmg','Pmg Name']].drop_duplicates()
df = df[(df.capacity>0)&(df.nsrp==1)]
df = df.loc[(df.capacity>0),('country','store','tpn','pmg','pmg_name','case_capacity','sold_units')].drop_duplicates()

# stores and PMG level
storeSold = df.groupby(['store','pmg'])['sold_units'].sum().reset_index()
storeSold.rename(columns={'sold_units':'total_sales'},inplace=True)
df2 = df.merge(storeSold, on=['store','pmg'],how='inner')
df2['case_capacity'] = np.where(df2.case_capacity>case_capacity_target,case_capacity_target,df2.case_capacity)
df2['case_capacity'] = np.where(((df2.pmg=='HDL28')&(df2.case_capacity<=6)), 30, df2.case_capacity) # TEMPORARY: HDL28 in here we can have icase = 1 but it should't be. We had not many lines in here
df2['capacity'] = df2.case_capacity * (df2.sold_units/df2.total_sales)
df2 = df2.groupby(['store','pmg'])['capacity'].sum().reset_index()
df2.rename(columns={'store':'Store','pmg':'Pmg'},inplace=True)

# just PMG level
pmgSold = df.groupby(['pmg'])['sold_units'].sum().reset_index()
pmgSold.rename(columns={'sold_units':'total_sales'},inplace=True)
df3 = df.merge(pmgSold, on=['pmg'],how='inner')
df3['Case_Capacity'] = np.where(df3.case_capacity>case_capacity_target,case_capacity_target,df3.case_capacity)
df3['capacity'] = df3.case_capacity * (df3.sold_units/df3.total_sales)
df3 = df3.groupby(['pmg'])['capacity'].sum().reset_index()
df3.rename(columns={'pmg':'Pmg', 'capacity':'pmgCapacity'},inplace=True)
df3 = store_inputs.merge(df3,on=('Pmg'),how='left')
df3[df3.isnull().any(axis=1)]['Pmg Name'].unique()

# how we can see above we have no values on the pmg names where the case capacity should be equal 1
df3['pmgCapacity'].fillna(1,inplace=True)

# **Combine store level and pmg level to check differences**
df4 = df2.merge(df3,on=['Store','Pmg'],how='right')
df4['capacity'] = np.where(df4['capacity'].isnull(), df4.pmgCapacity, df4.capacity)
df4['diff'] = df4.apply(lambda x: abs(x.pmgCapacity - x.capacity)/x.capacity,axis=1)

# we have around 2,4% of records with difference higher than 100%. So I will use higher case capacity number for those records
df4['Case_Capacity'] = np.where(df4['diff']>1, df4[["capacity", "pmgCapacity"]].min(axis=1), df4.capacity)
df4.drop(columns={'capacity', 'pmgCapacity', 'diff'},inplace=True)
df4['Case_Capacity'] = np.where(df4.Case_Capacity>case_capacity_target,case_capacity_target,df4.Case_Capacity)

df4.to_csv(directory_new / 'Model_Datasets/Case_Capacity_P8_2020.csv',index=False)

