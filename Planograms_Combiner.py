import pandas as pd
from pathlib import Path
import numpy as np

def PlanoCustomizer(df, country):
    if country == 'SK':
        c = "2"
    elif country == 'CZ':
        c = "1"
    elif country == 'PL':
        c = "3"
    else: 
        c = "4"    
    
    to_drop = ['DRG ID', 'DRG name', 'DRC ID', 'DRC name', 'Hyper DRG', 'Supplier']
    for d in to_drop:
        try:
            df = df.drop([d], axis=1)
        except:
            pass

    df = df.rename(index=str, columns={"Product TPN": "tpnb", "EAN code": "ean", "Product name": "pname", "Product status": "status", "Total Case Pack Size": "icase"})
    df = pd.melt(df, id_vars = [("tpnb"), ("ean"), ("pname"), ("status"), ("icase")], var_name = "store", value_name = "capacity")
    df = df.replace(np.nan, 0)
    df['store'] = c + df['store'].str[-4:]
    df['tpnb']=df['tpnb'].astype(str)
    df['ean']=df['ean'].astype(str)
    df['icase']=df['icase'].astype(int)
    df['store']=df['store'].astype(int)
    df['capacity']=df['capacity'].astype(int)
    return df

data_folder = Path("C:/D/Q32021/")
plano_file_name = 'FutureDatedShelfCap_All_Stores_20210331'

# # PL
# plano_file_PL = pd.read_csv(data_folder / f"{plano_file_name}_PL.txt", sep="|", encoding = 'unicode_escape', low_memory=False)
# plano_PL = PlanoCustomizer(plano_file_PL,'PL')

# SK
plano_file_SK = pd.read_csv(data_folder / f"{plano_file_name}_SK.txt", sep="|", encoding = 'unicode_escape', low_memory=False)
plano_SK = PlanoCustomizer(plano_file_SK,'SK')

# CZ
plano_file_CZ = pd.read_csv(data_folder / f"{plano_file_name}_CZ.txt", sep="|", encoding = 'unicode_escape', low_memory=False)
plano_CZ = PlanoCustomizer(plano_file_CZ,'CZ')

# HU
plano_file_HU = pd.read_csv(data_folder / f"{plano_file_name}_HU.txt", sep="|", encoding = 'unicode_escape', low_memory=False)
plano_HU = PlanoCustomizer(plano_file_HU,'HU')

plano_CE = plano_SK
plano_CE = plano_CE.append(plano_CZ, ignore_index = True)
plano_CE = plano_CE.append(plano_HU, ignore_index = True)
# plano_CE = plano_CE.append(plano_PL, ignore_index = True)
plano_CE = plano_CE[plano_CE['status'].str.contains("Live") == True]
plano_CE = (plano_CE[plano_CE.capacity > 0]).sort_values(by=['store'])

print(plano_CE.head())

#plano_CE.to_csv("Planogram_March2020.csv", sep=';', encoding='utf-8', index=False)
plano_CE.to_csv("C:/D/Q32021/Planogram_March2021.csv", index=False)