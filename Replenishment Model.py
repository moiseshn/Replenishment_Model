"""
Created on Mon Mar 30 09:27:28 2020
@author: mborycki

"""
from pathlib import Path
from datetime import datetime
import Replenishment_Model_Functions as rmf

def CurrentTime():
    now = datetime. now()
    current_time = now. strftime("%H:%M:%S")
    return int(current_time[6:8])

def CalcTime(time_start,time_stop,text):
    executed_time = time_stop-time_start if time_stop>time_start else ((60-time_start)+time_stop)
    return print(text+str(executed_time))

# File versions
version_base = 'actual'
version_saved = 'v7'

directory = Path("C:/D/#PRACA/ReplModel/")
repl_dataset_f = "Model_Datasets/Repl_Dataset_2019.zip"
dataset_inputs_f = 'Model_Datasets/stores_inputs_'+version_base+'.csv'
pallet_capacity_f = 'Model_Datasets/Pallet_Capacity.csv'
volumes_f = 'Model_Datasets/Volumes_2019_'+version_base+'.xlsx'
planogram_f = 'Model_Datasets/Planogram_IX_2019.zip'
items_sold_f = 'Model_Datasets/ItemsSold_X_2019.zip' # 30.09-27.10
stock_f = 'Model_Datasets/Stock_X_2019.zip' # 30.09-27.10
losses_f = 'Model_Datasets/Losses_X_2019.csv' # 30.09-27.10
ops_dev_f = 'Model_Datasets/OpsDev_XI_2019.zip'
most_f = 'Model_Datasets/MOST_Py.xlsx'

final_parameters_f = 'Parameters_Outputs/final_parameters_'+version_base+'.csv'
produce_parameters_f = 'Parameters_Outputs/produce_parameters_'+version_base+'.csv'
rtc_waste_f = 'Parameters_Outputs/Waste_RTC_'+version_base+'.csv'

items_f = 'RawData/sales_items_III_2020.csv'
cases_f = 'RawData/cases_delivered_III_2020.csv'
lines_f = 'RawData/product_stocked_III_2020.csv'

# Model Variables
RC_CAPACITY = (1+(1-0.62))
RC_DELIVERY = 0.23
RC_VS_PAL_CAPACITY = 0.62
PRE_SORT = 0.06
REX_ALLOWANCE = 4
SOLD_UNIT_DAYS = 7 # change 4
LOSS_UNIT_DAYS = 28
RC_Capacity_Ratio = (1+(1-0.62))
BACKSTOCK_TARGET = 0.4
CASE_CAP_TARGET = 40
MODULE_CRATES = 8
TABLE_CRATES = 4
SALES_CYCLE = (0.2, 0.2, 0.2, 0.2, 0.2)
FULFILL_TARGET = 0.6

# Function Variables
STORE_INPUTS_FUNC = True
STORE_INPUTS_FUNC_SAVE = False
VOLUMES_FUNC = False
VOLUMES_FUNC_SAVE = False
DATASET_TPN_FUNC = False
DATASET_TPN_FUNC_SAVE = False
PARAM_REPLENISHMENT_FUNC = False
PARAM_REPLENISHMENT_FUNC_SAVE = False
PARAM_PRODUCE_FUNC = False
PARAM_PRODUCE_FUNC_SAVE = False
PARAM_RTC_FUNC = False
PARAM_RTC_FUNC_SAVE = False
DRIVERS_REPLENISHMENT_FUNC = False
DRIVERS_REPLENISHMENT_FUNC_SAVE = False
DRIVERS_PRODUCE_FUNC = False
DRIVERS_PRODUCE_FUNC_SAVE = False
DRIVERS_RTC_FUNC = False
DRIVERS_RTC_FUNC_SAVE = False
DRIVERS_FINAL_FUNC = False
DRIVERS_FINAL_FUNC_SAVE = False
TIME_VALUES_FUNC = False
TIME_VALUES_FUNC_SAVE = False
MODEL_HOURS_FUNC = False
MODEL_HOURS_FUNC_SAVE = False

if STORE_INPUTS_FUNC == True:
    time_start = CurrentTime()
    store_inputs = rmf.StoreInputsCreator(directory)
    if STORE_INPUTS_FUNC_SAVE == True:
        store_inputs.to_csv(directory/'Model_Datasets/stores_inputs_'+version_saved+'.csv', index=False)
    time_stop = CurrentTime()
    CalcTime(time_start,time_stop,"Stores Inputs has been created and saved. Executed Time (sec): " if STORE_INPUTS_FUNC_SAVE == True else "Stores Inputs has been created and not saved. Executed Time (sec): ")
if VOLUMES_FUNC == True:
    time_start = CurrentTime()
    volumes = rmf.VolumesCreator(directory,items_f,cases_f,lines_f,dataset_inputs_f)
    if VOLUMES_FUNC_SAVE == True:
        volumes.to_excel(directory/'Model_Datasets/Volumes_2019_'+version_saved+'.xlsx', index=False)
    time_stop = CurrentTime()
    CalcTime(time_start,time_stop,"Volumes_2019 has been created and saved. Executed Time (sec): " if VOLUMES_FUNC_SAVE == True else "Volumes_2019 has been created and not saved. Executed Time (sec): ")
if DATASET_TPN_FUNC == True:
    time_start = CurrentTime()
    dataset_tpn = rmf.ReplDatasetTpn(directory,dataset_inputs_f,planogram_f,stock_f,ops_dev_f,items_sold_f)
    if DATASET_TPN_FUNC_SAVE == True:
        dataset_tpn.to_csv(directory / 'Model_Datasets/Repl_Dataset_2019_'+version_saved+'.csv', index=False)
    time_stop = CurrentTime()
    CalcTime(time_start,time_stop,"Replenishment Dataset TPN table has been created and saved. Executed Time (sec): " if DATASET_TPN_FUNC_SAVE == True else "Replenishment Dataset TPN table has been created and not saved. Executed Time (sec): ")
if PARAM_REPLENISHMENT_FUNC == True:
    time_start = CurrentTime()
    Parameters_Repl = rmf.ReplenishmentParameters(directory,repl_dataset_f,SOLD_UNIT_DAYS,BACKSTOCK_TARGET,CASE_CAP_TARGET,dataset_inputs_f,pallet_capacity_f,volumes_f)
    if PARAM_REPLENISHMENT_FUNC_SAVE == True:
        Parameters_Repl.to_csv(directory / 'Parameters_Outputs/final_parameters_'+version_saved+'.csv', index=False)
    time_stop = CurrentTime()
    CalcTime(time_start,time_stop,"Replenishment Parameters table has been created and saved. Executed Time (sec): " if PARAM_REPLENISHMENT_FUNC_SAVE == True else "Replenishment Parameters table has been created and not saved. Executed Time (sec): ")
if PARAM_PRODUCE_FUNC == True:
    time_start = CurrentTime()
    Parameters_Produce = rmf.ProduceParameters(directory,repl_dataset_f,dataset_inputs_f,SOLD_UNIT_DAYS,MODULE_CRATES,TABLE_CRATES,volumes_f,SALES_CYCLE,FULFILL_TARGET,pallet_capacity_f)
    if PARAM_PRODUCE_FUNC_SAVE == True:
        Parameters_Produce.to_csv(directory / 'Parameters_Outputs/produce_parameters_'+version_saved+'.csv', index=False)
    time_stop = CurrentTime()
    CalcTime(time_start,time_stop,"Produce Parameters has been created and saved. Executed Time (sec): " if PARAM_PRODUCE_FUNC_SAVE == True else "Produce Parameters has been created and not saved. Executed Time (sec): ")
if PARAM_RTC_FUNC == True: # dodaj odtąd opcję z czasem i textem jak wyżej: CalcTime
    Parameters_Rtc_Waste = rmf.RtcParameters(directory,repl_dataset_f,losses_f,LOSS_UNIT_DAYS)
    CurrentTime("RTC Parameters are ready")
    if PARAM_RTC_FUNC_SAVE == True:
        Parameters_Rtc_Waste.to_csv(directory / 'Parameters_Outputs/Waste_RTC_',version_saved,'.csv', index=False)
        CurrentTime("RTC Parameters table has been saved")
if DRIVERS_REPLENISHMENT_FUNC == True:
    Repl_Drivers = rmf.ReplenishmentDrivers(directory,final_parameters_f,dataset_inputs_f,RC_CAPACITY)
    CurrentTime("Replenishment Drivers are ready")
    if DRIVERS_REPLENISHMENT_FUNC_SAVE == True:
        Repl_Drivers.to_csv(directory / 'Drivers_Replenishment_'+version_saved+'.csv', index=False)
        CurrentTime("Replenishment Drivers table has been saved")  
if DRIVERS_PRODUCE_FUNC == True:
    Produce_Drivers = rmf.ProduceDrivers(directory,produce_parameters_f,RC_DELIVERY,RC_VS_PAL_CAPACITY)
    CurrentTime("Produce Drivers are ready")
    if DRIVERS_PRODUCE_FUNC_SAVE == True:
        Produce_Drivers.to_csv(directory / 'Drivers_Produce_'+version_saved+'.csv', index=False)
        CurrentTime("Produce Drivers table has been saved")  
if DRIVERS_RTC_FUNC == True:
    Rtc_Drivers = rmf.RtcDrivers(directory,rtc_waste_f,dataset_inputs_f)
    CurrentTime("RTC table is ready")
    if DRIVERS_RTC_FUNC_SAVE == True:
        Rtc_Drivers.to_csv(directory / 'Drivers_RTC_'+version_saved+'.csv', index=False)
        CurrentTime("RTC table has been saved")  
if DRIVERS_FINAL_FUNC == True:
    Final_Drivers = rmf.FinalizingDrivers(directory,dataset_inputs_f,produce_parameters_f,Repl_Drivers,Produce_Drivers,Rtc_Drivers)
    CurrentTime("Final Drivers table is ready")
    if DRIVERS_FINAL_FUNC_SAVE == True:
        Final_Drivers.to_csv(directory / 'Drivers_Final_'+version_saved+'.csv', index=False)
        CurrentTime("Final Drivers table has been saved")   
if TIME_VALUES_FUNC == True:
    Time_Value = rmf.TimeValues(directory,dataset_inputs_f,most_f,Final_Drivers)
    Time_Value = rmf.HoursCalculation(directory,dataset_inputs_f,Time_Value,REX_ALLOWANCE) # the same table like above but with hours (overwritten)
    CurrentTime("Time Values table with hours is ready")
    if TIME_VALUES_FUNC_SAVE == True:
        Time_Value.to_csv(directory / 'Time_Value_'+version_saved+'.csv', index=False)
        CurrentTime("Time Values table has been saved")     
if MODEL_HOURS_FUNC == True:
    Model_Hours = rmf.ModelHours(directory,Time_Value)
    CurrentTime("Model Hours table is ready")
    if MODEL_HOURS_FUNC_SAVE == True:
        Model_Hours.to_csv(directory / 'Model_Outputs_'+version_saved+'.csv', index=False)
        CurrentTime("Model Hours table has been saved") 
