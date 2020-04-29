import pandas as pd
from pandas import DataFrame, read_excel, merge, melt
from pathlib import Path
import numpy as np


data_folder = Path("C:/D/#PRACA/2020/Colleague Requests/4.April/Backstock_032020/")
# SK
plano_SK = pd.read_csv(data_folder / "FutureDatedShelfCap_All_Stores_20200318_SK.txt", sep="|", encoding = 'unicode_escape', low_memory=False)
plano_SK = plano_SK.drop(['DRG ID', 'DRG name', 'DRC ID', 'DRC name', 'Hyper DRG', 'Supplier'], axis=1)
plano_SK = plano_SK.rename(index=str, columns={"Product TPN": "tpnb", "EAN code": "ean", "Product name": "pname", "Product status": "status", "Total Case Pack Size": "icase"})
plano_SK = pd.melt(plano_SK, id_vars = [("tpnb"), ("ean"), ("pname"), ("status"), ("icase")], var_name = "store", value_name = "capacity")
plano_SK = plano_SK.replace(np.nan, 0)
plano_SK['store'] = "2" + plano_SK['store'].str[-4:]
plano_SK['tpnb']=plano_SK['tpnb'].astype(str)
plano_SK['ean']=plano_SK['ean'].astype(str)
plano_SK['icase']=plano_SK['icase'].astype(int)
plano_SK['store']=plano_SK['store'].astype(int)
plano_SK['capacity']=plano_SK['capacity'].astype(int)
# CZ
plano_CZ = pd.read_csv(data_folder / "FutureDatedShelfCap_All_Stores_20200318_CZ.txt", sep="|", encoding = 'unicode_escape', low_memory=False)
plano_CZ = plano_CZ.drop(['DRG ID', 'DRG name', 'DRC ID', 'DRC name', 'Hyper DRG', 'Supplier'], axis=1)
plano_CZ = plano_CZ.rename(index=str, columns={"Product TPN": "tpnb", "EAN code": "ean", "Product name": "pname", "Product status": "status", "Total Case Pack Size": "icase"})
plano_CZ = pd.melt(plano_CZ, id_vars = [("tpnb"), ("ean"), ("pname"), ("status"), ("icase")], var_name = "store", value_name = "capacity")
plano_CZ = plano_CZ.replace(np.nan, 0)
plano_CZ['store'] = "1" + plano_CZ['store'].str[-4:]
plano_CZ['tpnb']=plano_CZ['tpnb'].astype(str)
plano_CZ['ean']=plano_CZ['ean'].astype(str)
plano_CZ['icase']=plano_CZ['icase'].astype(int)
plano_CZ['store']=plano_CZ['store'].astype(int)
plano_CZ['capacity']=plano_CZ['capacity'].astype(int)
# HU ("row_id" - additionally)
plano_HU = pd.read_csv(data_folder / "FutureDatedShelfCap_All_Stores_20200318_HU.txt", sep="|", encoding = 'unicode_escape', low_memory=False)
plano_HU = plano_HU.drop(['DRG ID', 'DRG name', 'DRC ID', 'DRC name', 'Supplier'], axis=1)
plano_HU = plano_HU.rename(index=str, columns={"Product TPN": "tpnb", "EAN code": "ean", "Product name": "pname", "Product status": "status", "Total Case Pack Size": "icase"})
plano_HU = pd.melt(plano_HU, id_vars = [("tpnb"), ("ean"), ("pname"), ("status"), ("icase")], var_name = "store", value_name = "capacity")
plano_HU = plano_HU.replace(np.nan, 0)
plano_HU['store'] = "4" + plano_HU['store'].str[-4:]
#plano_HU['tpnb']=plano_HU['tpnb'].astype(int) # 'Product TPN' has float as a data type, so on th end of the number we have ".0"
plano_HU['tpnb']=plano_HU['tpnb'].astype(str)
plano_HU['ean']=plano_HU['ean'].astype(str)
plano_HU['icase']=plano_HU['icase'].astype(int)
plano_HU['store']=plano_HU['store'].astype(int)
plano_HU['capacity']=plano_HU['capacity'].astype(int)
# PL
plano_PL = pd.read_csv(data_folder / "FutureDatedShelfCap_All_Stores_20200318_PL.txt", sep="|", encoding = 'unicode_escape', low_memory=False)
plano_PL = plano_PL.drop(['DRG ID', 'DRG name', 'DRC ID', 'DRC name', 'Supplier'], axis=1)
plano_PL = plano_PL.rename(index=str, columns={"Product TPN": "tpnb", "EAN code": "ean", "Product name": "pname", "Product status": "status", "Total Case Pack Size": "icase"})
plano_PL = pd.melt(plano_PL, id_vars = [("tpnb"), ("ean"), ("pname"), ("status"), ("icase")], var_name = "store", value_name = "capacity")
plano_PL = plano_PL.replace(np.nan, 0)
plano_PL['store'] = "3" + plano_PL['store'].str[-4:]
plano_PL['tpnb']=plano_PL['tpnb'].astype(str)
plano_PL['ean']=plano_PL['ean'].astype(str)
plano_PL['icase']=plano_PL['icase'].astype(int)
plano_PL['store']=plano_PL['store'].astype(int)
plano_PL['capacity']=plano_PL['capacity'].astype(int)
plano_CE = plano_SK
plano_CE = plano_CE.append(plano_CZ, ignore_index = True)
plano_CE = plano_CE.append(plano_HU, ignore_index = True)
plano_CE = plano_CE.append(plano_PL, ignore_index = True)
plano_CE = plano_CE[plano_CE['status'].str.contains("Live") == True]
plano_CE = (plano_CE[plano_CE.capacity > 0])
len(plano_CE.index)
plano_CE.dtypes
plano_feb = plano_CE.sort_values(by=['store'])
plano_CE.head()
plano_feb.to_csv("Planogram_March2020.csv", sep=';', encoding='utf-8', index=False)