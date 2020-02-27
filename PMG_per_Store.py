import pandas as pd
from pathlib import Path

folder = Path('C:/D/#PRACA/2020/Inputs/Database/B.Customized_Files/')

pmg = pd.read_excel(folder / "Dataset_Inputs.xlsx", sheet_name='pmg')
pmg = pmg[pmg.area=='Replenishment']
pmg = pmg[['pmg']].drop_duplicates().sort_values('pmg')

storelist = pd.read_excel(folder / "Dataset_Inputs.xlsx", sheet_name='store_list', usecols=['store'])

df = pd.DataFrame({'store': [], 'pmg': []})
for i in storelist.index: 
    a = storelist['store'][i]
    for j in pmg.index:
        b = pmg['pmg'][j]
        df = df.append({'store': a, 'pmg': b}, ignore_index=True)

df.store = df.store.astype(int)
df.to_excel('stores_and_pmg.xlsx', index=False)