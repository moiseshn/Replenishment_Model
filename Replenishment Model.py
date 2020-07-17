import pandas as pd
from pathlib import Path
from datetime import datetime
import Replenishment_Model_Functions as rmf
import What_If_Functions as what_if

def CurrentTime():
    now = datetime. now()
    current_time = now. strftime("%H:%M:%S")
    h = int(current_time[0:2]) # time format: '11:53:12'
    m = int(current_time[3:5])
    s = int(current_time[6:8])
    sec_time = h*3600+m*60+s
    return sec_time
def CalcTime(time_start,time_stop,text):
    executed_time = time_stop-time_start
    return print(text + str(executed_time))
startCode = CurrentTime()

# File versions
version_base = 'Q3'
version_saved = 'Q3_v2' 

directory = Path("C:/D/#PRACA/ReplModel/")
repl_dataset_f = "Model_Datasets/Repl_Dataset_2019.zip"
planogram_f = 'Model_Datasets/Planogram_IX_2019.zip'
items_sold_f = 'Model_Datasets/ItemsSold_X_2019.zip' # 30.09-27.10
stock_f = 'Model_Datasets/Stock_X_2019.zip' # 30.09-27.10
ops_dev_f = 'Model_Datasets/OpsDev_XI_2019.zip'

volumes_f = f'Model_Datasets/Volumes_2019_{version_base}.xlsx' # Volumes_2019_adjusted2_newStores.xlsx (the same volumes like in P82019 + new stores)
excel_inputs_f = f'Model_Inputs/Stores_Inputs_{version_base}.xlsx' 
pallet_capacity_f = f'Model_Datasets/Pallet_Capacity_{version_base}.csv'
case_capacity_f = f'Model_Datasets/Case_Capacity_{version_base}.csv' 
losses_f = f'Model_Datasets/Losses_X_2019_{version_base}.csv' # 30.09-27.10 + code 14
most_f = f'Model_Inputs/MOST_Replenishment_{version_base}.xlsb' 

act_model_outputs = f'Model_Outputs/Model_Outputs_Q3_v1.xlsx' # Budgeted + ~2hrs on Produce because of Heavy/Light issue described in the summary file

items_f = 'RawData/volumes_III_2020/sales_items_III_2020.csv'
cases_f = 'RawData/volumes_III_2020/cases_delivered_III_2020.csv'
lines_f = 'RawData/volumes_III_2020/product_stocked_III_2020.csv'

# Model Variables
RC_CAPACITY = (1+(1-0.62))
RC_DELIVERY = 0.23
RC_VS_PAL_CAPACITY = 0.62
PRE_SORT = 0.06
REX_ALLOWANCE = 4
SOLD_UNIT_DAYS = 7
LOSS_UNIT_DAYS = 28
RC_Capacity_Ratio = (1+(1-0.62))
BACKSTOCK_TARGET = 0.4
CASE_CAP_TARGET = 40
MODULE_CRATES = 8
TABLE_CRATES = 4
SALES_CYCLE = (0.2, 0.2, 0.2, 0.2, 0.2)
FULFILL_TARGET = 0.6

# Model
RUN_MODEL = True

# Datasets
VOLUMES_FUNC = False
VOLUMES_SAVE = False

DATASET_TPN_FUNC = False
DATASET_TPN_SAVE = False

# Save 
OPB_DEP_SAVE = True
OPB_DIV_SAVE = False
INSIGHT_SAVE = False
EXCEL_DRIVERS_SAVE = False
MODEL_DRIVERS_SAVE = False

# BI Report
BI_REPORT = False
REPL_TYPE = False # if we put True then we need a new ReplDataset table to calc New Repl Types

# Create a dataframe for excel file with Drivers/Profiles
store_inputs = rmf.StoreInputsCreator(directory,excel_inputs_f)

if VOLUMES_FUNC == True:
    time_start = CurrentTime()
    volumes = rmf.VolumesCreator(directory,items_f,cases_f,lines_f,store_inputs)
    if VOLUMES_SAVE == True:
        file_name = f'Model_Datasets/Volumes_2019_{version_saved}.xlsx'
        volumes.to_excel(directory/file_name, index=False)
    time_stop = CurrentTime()
    CalcTime(time_start,time_stop,"Volumes_2019 has been created and saved. Executed Time (sec): " if VOLUMES_SAVE == True else "Volumes_2019 has been created and not saved. Executed Time (sec): ")
if DATASET_TPN_FUNC == True:
    time_start = CurrentTime()
    dataset_tpn = rmf.ReplDatasetTpn(directory,store_inputs,planogram_f,stock_f,ops_dev_f,items_sold_f)
    if DATASET_TPN_SAVE == True:
        file_name = f'Model_Datasets/Repl_Dataset_2019_{version_saved}.csv'
        dataset_tpn.to_csv(directory / file_name, index=False)
    time_stop = CurrentTime()
    CalcTime(time_start,time_stop,"Replenishment Dataset TPN table has been created and saved. Executed Time (sec): " if DATASET_TPN_SAVE == True else "Replenishment Dataset TPN table has been created and not saved. Executed Time (sec): ")

# Run the Replenishment Model:
if RUN_MODEL == True:
    # Parameters Calculations
    time_start = CurrentTime()
    Repl_Dataset = pd.read_csv(directory/repl_dataset_f,compression='zip')
    Parameters_Repl = rmf.ReplenishmentParameters(directory,Repl_Dataset,SOLD_UNIT_DAYS,BACKSTOCK_TARGET,CASE_CAP_TARGET,store_inputs,pallet_capacity_f,volumes_f,case_capacity_f) 
    Repl_Drivers = rmf.ReplenishmentDrivers(Parameters_Repl,store_inputs,RC_CAPACITY)
    time_stop = CurrentTime()
    CalcTime(time_start,time_stop,"Replenishment Parameters table is ready. Executed Time (sec): ")
    
    time_start = CurrentTime()
    Parameters_Produce = rmf.ProduceParameters(directory,Repl_Dataset,excel_inputs_f,SOLD_UNIT_DAYS,MODULE_CRATES,TABLE_CRATES,volumes_f,SALES_CYCLE,FULFILL_TARGET,pallet_capacity_f)
    Produce_Drivers = rmf.ProduceDrivers(directory,Parameters_Produce,RC_DELIVERY,RC_VS_PAL_CAPACITY)
    time_stop = CurrentTime()
    CalcTime(time_start,time_stop,"Produce Parameters table is ready. Executed Time (sec): ")
    
    time_start = CurrentTime()
    Parameters_Rtc_Waste = rmf.RtcParameters(directory,Repl_Dataset,losses_f,LOSS_UNIT_DAYS)
    Rtc_Drivers = rmf.RtcDrivers(Parameters_Rtc_Waste,store_inputs)
    time_stop = CurrentTime()
    CalcTime(time_start,time_stop,"RTC Table is ready. Executed Time (sec): ")
    
    # Finalizing Drivers
    Final_Drivers = rmf.FinalizingDrivers(directory,store_inputs,Parameters_Produce,Repl_Drivers,Produce_Drivers,Rtc_Drivers)

    # Combining Times, Drivers and calc hours per activity
    time_start = CurrentTime()
    Time_Value = rmf.TimeValues(directory,store_inputs,most_f,Final_Drivers)
    Time_Value = rmf.HoursCalculation(directory,store_inputs,Time_Value,REX_ALLOWANCE) # the same table like above but with hours (overwritten)
    time_stop = CurrentTime()
    CalcTime(time_start,time_stop,"Time Values Dataframe is ready. Executed Time (sec): ")
    
    # Summarizing Hours
    Time_Value, Final_Drivers = rmf.NewStoresQ3(Time_Value,Final_Drivers)
    rmf.OutputsComparison(directory,Time_Value,act_model_outputs)
    opb_dep,opb_div,insight,final_drivers_csv = rmf.OperationProductivityBasics(Time_Value,Final_Drivers)

    # Saving Tables from the model
    if EXCEL_DRIVERS_SAVE == True:
        file_name = f'Model_Outputs/Drivers_Final_{version_saved}.xlsx'
        stores_profiles = pd.read_csv(directory/store_inputs) 
        stores_profiles = stores_profiles[['Country','Store','Store Name','Plan Size','Format']].drop_duplicates() # in excel model we need these info
        Final_Drivers_xlsx = Final_Drivers.copy()
        Final_Drivers_xlsx = Final_Drivers_xlsx.merge(stores_profiles,on='Store',how='left')
        Final_Drivers_xlsx['Dep'] = Final_Drivers_xlsx.Dep.apply(lambda x: 'GM' if x=='HDL' else x) # in the excel model I use GM instead HDL
        Final_Drivers_xlsx.to_excel(directory / file_name, index=False)
    
    # Outputs saving 
    if OPB_DEP_SAVE == True:
        file_name = f'Model_Outputs/OPB_DEP_{version_saved}.xlsx'
        opb_dep.to_excel(directory / file_name, index=False)
    if OPB_DIV_SAVE == True:
        file_name = f'Model_Outputs/OPB_DIV_{version_saved}.xlsx'
        opb_div.to_excel(directory / file_name, index=False)
    if INSIGHT_SAVE == True:
        file_name = f'Model_Outputs/INSIGHT_{version_saved}.csv'
        insight.to_csv(directory / file_name, index=False)
    if MODEL_DRIVERS_SAVE == True:
        file_name = f'Model_Outputs/DRIVERS_{version_saved}.csv'
        final_drivers_csv.to_csv(directory / file_name, index=False)
    if BI_REPORT == True:
        #CalcTime(time_start,time_stop,"BI Report Inputs are saving now. Executed Time (sec): ")
        if REPL_TYPE == True:
            rmf.ReportBi(directory,Time_Value,REPL_TYPE,Repl_Dataset)
        else:
            Repl_Dataset = False
            rmf.ReportBi(directory,Time_Value,REPL_TYPE,Repl_Dataset)

# Calc the final time for running the script
endCode = CurrentTime()
minCode = ((endCode-startCode)/60)//1
secCode = round((((endCode-startCode)/60) - minCode)*60)
print(f'Total Executed Time: {minCode} min {secCode} sec')