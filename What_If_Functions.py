"""
Here you can find all the what if calc from 2020 (since May)
@mborycki
"""
import pandas as pd
import numpy as np

def InnerPack(folder,dataset):
	"""
	Topic: "inner packs": what if we will keep some cases of a chosen tpns in a big carton?
	Date of the request: 24.04.2020
	Author of the request: Peter Gurban
	"""
	inner_case = 'C:/D/#PRACA/2020/Colleague Requests/4.April/InnerCase/CE_inner packs.xlsx'
	inner_case_df = pd.read_excel(inner_case,sheet_name='CE')
	Repl_Dataset = pd.read_csv(folder/dataset,compression='zip')
	Repl_Dataset = Repl_Dataset.merge(inner_case_df,on=['country','tpnb'],how='left')
	Repl_Dataset['new_stock'] = np.where(Repl_Dataset['total pcs']>Repl_Dataset.capacity,(Repl_Dataset.capacity*0.25)+Repl_Dataset['total pcs'],Repl_Dataset.stock)
	Repl_Dataset['new_stock'] = np.where(Repl_Dataset.new_stock<Repl_Dataset.stock,Repl_Dataset.stock,Repl_Dataset.new_stock)
	Repl_Dataset['CartonNo'] = ((Repl_Dataset.sold_units / Repl_Dataset.case_capacity) / Repl_Dataset['inner packs in case']).round(0)
	Repl_Dataset['CartonNo'] = Repl_Dataset['CartonNo'].apply(lambda x: 1 if x < 1 else x)
	Repl_Dataset = Repl_Dataset.fillna(0)
# =============================================================================
# 	# here we can see how many cartons need to be open
# 	CartonNo_df = Repl_Dataset[['country', 'store', 'tpn', 'tpnb', 'name', 'pmg', 'pmg_name', 'division',
# 		   'case_capacity', 'capacity', 'sold_units', 'stock', 'new_stock', 'inner packs in case', 'case size', 'total pcs', 'CartonNo']].copy()
# 	CartonNo_df = CartonNo_df[CartonNo_df.CartonNo>0]
# 	CartonNo_df.to_excel('InnerCase_Dataset.xlsx',index=False)
# =============================================================================
	Repl_Dataset.drop(columns='stock',inplace=True)
	Repl_Dataset.rename(columns={'new_stock':'stock'},inplace=True)
	Repl_Dataset.drop(columns={'name','inner packs in case', 'case size', 'total pcs', 'CartonNo'},inplace=True)
	return Repl_Dataset