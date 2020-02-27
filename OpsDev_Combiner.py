import pandas as pd
from pathlib import Path
import numpy as np
import matplotlib.pyplot as plt

folder = Path('d:/#PRACA/2020/Inputs/Database/OpsDev/')
# =============================================================================
# Add function in here
# =============================================================================
cz_number = 10000
sk_number = 20000
hu_number = 40000
pl_number = 30000

srp_cz = pd.read_csv(folder / "CZ Merch Style 31.10.19.csv", low_memory=False, usecols=['FP_NO', 'MERCHSTYLE', 'ART_ID'])
srp_cz = srp_cz.rename(columns={'FP_NO': 'store', 'MERCHSTYLE': 'srp', 'ART_ID': 'tpnb'})
srp_cz['store'] = cz_number + srp_cz.store
srp_cz['srp'] = np.where(srp_cz.srp > 1, 0, srp_cz.srp)
srp_sk = pd.read_csv(folder / "SK Merch Style 31.10.19.csv", low_memory=False, usecols=['FP_NO', 'MERCHSTYLE', 'ART_ID'])
srp_sk = srp_sk.rename(columns={'FP_NO': 'store', 'MERCHSTYLE': 'srp', 'ART_ID': 'tpnb'})
srp_sk['store'] = sk_number + srp_sk.store
srp_sk['srp'] = np.where(srp_sk.srp > 1, 0, srp_sk.srp)
srp_hu = pd.read_csv(folder / "HU Merch Style 31.10.19.csv", low_memory=False, usecols=['FP_NO', 'MERCHSTYLE', 'ART_ID'])
srp_hu = srp_hu.rename(columns={'FP_NO': 'store', 'MERCHSTYLE': 'srp', 'ART_ID': 'tpnb'})
srp_hu['store'] = hu_number + srp_hu.store
srp_hu['srp'] = np.where(srp_hu.srp > 1, 0, srp_hu.srp)
srp_pl = pd.read_csv(folder / "PL Merch Style 31.10.19.csv", low_memory=False, usecols=['FP_NO', 'MERCHSTYLE', 'ART_ID'])
srp_pl = srp_pl.rename(columns={'FP_NO': 'store', 'MERCHSTYLE': 'srp', 'ART_ID': 'tpnb'})
srp_pl['store'] = pl_number + srp_pl.store
srp_pl['srp'] = np.where(srp_pl.srp > 1, 0, srp_pl.srp)
srp_ce = pd.concat([srp_cz, srp_sk, srp_hu, srp_pl])
srp_ce = srp_ce[['store', 'tpnb', 'srp']]
srp_ce['tpnb'] = srp_ce['tpnb'].astype(str)

col_name = ['store', 'tpnb']
full_pallets = pd.read_excel(folder / 'CE_full_pallets.xlsx', sheet_name='CE', names = col_name)
full_pallets['tpnb'] = full_pallets['tpnb'].astype(str)
full_pallets['full_pallet'] = 1

mu = pd.read_excel(folder / 'HU_MU.xlsx')
mu = mu[col_name]
mu['tpnb'] = mu['tpnb'].astype(str)
mu['mu'] = 1

foil = pd.read_excel(folder / 'foil_replenishment.xlsx', sheet_name='CE', names = col_name)
foil['tpnb'] = foil['tpnb'].astype(str)
foil['foil'] = 1

half_pallets = pd.read_excel(folder / 'PL_half_pallets.xlsx')
half_pallets = half_pallets[col_name]
half_pallets['tpnb'] = half_pallets['tpnb'].astype(str)
half_pallets['half_pallet'] = 1

split_pallets = pd.read_excel(folder / 'split_pallets_beer.xlsx', sheet_name='CE', names = col_name)
split_pallets['tpnb'] = split_pallets['tpnb'].astype(str)
split_pallets['split_pallet'] = 1

ops_dev_table = srp_ce.merge(full_pallets, on=['store', 'tpnb'], how='outer')
ops_dev_table = ops_dev_table.replace(np.nan, 0)
ops_dev_table = ops_dev_table.groupby(['store', 'tpnb']).sum().reset_index()
ops_dev_table['srp'] = np.where(((ops_dev_table.srp > 0) & (ops_dev_table.full_pallet > 0)), 0, ops_dev_table.srp)
ops_dev_table = ops_dev_table.drop_duplicates()

ops_dev_table = ops_dev_table.merge(mu, on=['store', 'tpnb'], how='outer')
ops_dev_table = ops_dev_table.replace(np.nan, 0)
ops_dev_table = ops_dev_table.groupby(['store', 'tpnb']).sum().reset_index()
ops_dev_table['srp'] = np.where(((ops_dev_table.srp > 0) & (ops_dev_table.mu > 0)), 0, ops_dev_table.srp)
ops_dev_table['mu'] = np.where(((ops_dev_table.full_pallet > 0) & (ops_dev_table.mu > 0)), 0, ops_dev_table.mu)
ops_dev_table = ops_dev_table.drop_duplicates()

ops_dev_table = ops_dev_table.merge(foil, on=['store', 'tpnb'], how='outer')
ops_dev_table = ops_dev_table.replace(np.nan, 0)
ops_dev_table = ops_dev_table.groupby(['store', 'tpnb']).sum().reset_index()
ops_dev_table['srp'] = np.where(((ops_dev_table.srp > 0) & (ops_dev_table.foil > 0)), 0, ops_dev_table.srp)
ops_dev_table['foil'] = np.where(((ops_dev_table.full_pallet > 0) & (ops_dev_table.foil > 0)), 0, ops_dev_table.foil)
ops_dev_table['foil'] = np.where(((ops_dev_table.mu > 0) & (ops_dev_table.foil > 0)), 0, ops_dev_table.foil)
ops_dev_table = ops_dev_table.drop_duplicates()

ops_dev_table = ops_dev_table.merge(half_pallets, on=['store', 'tpnb'], how='outer')
ops_dev_table = ops_dev_table.replace(np.nan, 0)
ops_dev_table = ops_dev_table.groupby(['store', 'tpnb']).sum().reset_index()
ops_dev_table['total'] = ops_dev_table.full_pallet + ops_dev_table.mu + ops_dev_table.srp
ops_dev_table['half_pallet'] = np.where(ops_dev_table.total > 0, 0, ops_dev_table.half_pallet)
ops_dev_table = ops_dev_table.drop_duplicates()

ops_dev_table = ops_dev_table.merge(split_pallets, on=['store', 'tpnb'], how='outer')
ops_dev_table = ops_dev_table.replace(np.nan, 0)
ops_dev_table = ops_dev_table.groupby(['store', 'tpnb']).sum().reset_index()
ops_dev_table['total'] = ops_dev_table.total + ops_dev_table.half_pallet
ops_dev_table['split_pallet'] = np.where(ops_dev_table.total > 0, 0, ops_dev_table.split_pallet)
ops_dev_table = ops_dev_table.drop_duplicates()
ops_dev_table['nsrp'] = np.where(ops_dev_table.total == 0, 1, 0)
ops_dev_table = ops_dev_table.drop('total', 1)
ops_dev_table['srp'] = ops_dev_table['srp'].astype(int)
ops_dev_table['full_pallet'] = ops_dev_table['full_pallet'].astype(int)
ops_dev_table['mu'] = ops_dev_table['mu'].astype(int)
ops_dev_table['foil'] = ops_dev_table['foil'].astype(int)
ops_dev_table['half_pallet'] = ops_dev_table['half_pallet'].astype(int)
ops_dev_table['split_pallet'] = ops_dev_table['split_pallet'].astype(int)
ops_dev_table['nsrp'] = ops_dev_table['nsrp'].astype(int)
ops_dev_table['srp'] = np.where(ops_dev_table.srp > 0, 1, 0)
ops_dev_table['full_pallet'] = np.where(ops_dev_table.full_pallet > 0, 1, 0)
ops_dev_table['mu'] = np.where(ops_dev_table.mu > 0, 1, 0)
ops_dev_table['foil'] = np.where(ops_dev_table.foil > 0, 1, 0)
ops_dev_table['half_pallet'] = np.where(ops_dev_table.half_pallet > 0, 1, 0)
ops_dev_table['split_pallet'] = np.where(ops_dev_table.split_pallet > 0, 1, 0)

result = len(ops_dev_table.drop_duplicates()) - len(ops_dev_table.drop_duplicates(subset=['store', 'tpnb'])) # checking do we have more than 1 value for the same store / tpn
if result == 0:
    print('OK')
else:
    print('We have problem. There is:', result, 'records with different values')

ops_dev_table.sort_values('tpnb').tail(20) #remove tpnb whiche are not numbers
ops_dev_table = ops_dev_table[(ops_dev_table.tpnb != 'new 186') & (ops_dev_table.tpnb != 'D100385047') & (ops_dev_table.tpnb != 'D5902327061144')]
ops_dev_table[ops_dev_table.duplicated(subset=['store', 'tpnb'], keep=False)]
ops_dev_table.to_csv('ops_dev_november19.csv', sep=';', index=False)