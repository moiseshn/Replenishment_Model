"""
Created on Mon Mar 30 09:27:28 2020
@author: mborycki

"""
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

# File versions
version_base = 'actual'

directory = Path("C:/D/#PRACA/ReplModel/")
csv_inputs_f = 'Model_Datasets/stores_inputs_'+version_base+'.csv'
most_f = 'Model_Datasets/MOST_Py.xlsx'
act_model_outputs = 'Model_Outputs/Model_Outouts_actual.xlsx'

final_parameters_f = 'Parameters_Outputs/final_parameters_'+version_base+'.csv'
produce_parameters_f = 'Parameters_Outputs/produce_parameters_'+version_base+'.csv'
rtc_waste_f = 'Parameters_Outputs/Waste_RTC_'+version_base+'.csv'

# Model Variables
RC_CAPACITY = (1+(1-0.62))
RC_DELIVERY = 0.23
RC_VS_PAL_CAPACITY = 0.62
REX_ALLOWANCE = 4

"""
KPI Calculator:
It is a function shows effect if we will change some parameters such as Backstock Ratio. 
E.g what will be a cost if we will increase backstock by 1%?
I sent the outputs to KPI Calculator which is helpful for our business in making some decisions
"""
repl_type_list = ['Full_Pallet_ratio','NSRP_ratio','SRP_ratio','Backstock_ratio'] # <- KPI Calculator
volume_list = ['Cases Delivered','Active Lines'] # <- KPI Calculator
profile_list = ['Number of Modules','Distance: WH to SF']
kpi_list = ['Full_Pallet_ratio','NSRP_ratio','SRP_ratio','Backstock_ratio','Cases Delivered','Active Lines','Number of Modules','Distance: WH to SF']

outputs_df = pd.read_excel(directory/act_model_outputs) # <- KPI Calculator
outputs_df = outputs_df[['Country','Store','Dep']].drop_duplicates() # <- KPI Calculator
for kpi in kpi_list:
    print(kpi + ' is running...: \n') # <- KPI Calculator
    time_start = CurrentTime()
    Parameters_Repl = pd.read_csv(directory/final_parameters_f)
    Repl_Drivers = rmf.ReplenishmentDrivers(directory,Parameters_Repl,csv_inputs_f,RC_CAPACITY)
    Parameters_Produce = pd.read_csv(directory/produce_parameters_f)
    Produce_Drivers = rmf.ProduceDrivers(directory,Parameters_Produce,RC_DELIVERY,RC_VS_PAL_CAPACITY)
    Parameters_Rtc_Waste = pd.read_csv(directory/rtc_waste_f)
    Rtc_Drivers = rmf.RtcDrivers(directory,Parameters_Rtc_Waste,csv_inputs_f)
    
    # Finalizing Drivers <- KPI Calculator
    Final_Drivers = rmf.FinalizingDrivers(directory,csv_inputs_f,produce_parameters_f,Repl_Drivers,Produce_Drivers,Rtc_Drivers)
    if kpi in repl_type_list:
        what_if.KpiReplType(Final_Drivers,kpi)
    elif kpi in volume_list:
        what_if.KpiVolumes(Final_Drivers,kpi)
    else:
        what_if.KpiProfiles(Final_Drivers,kpi)

    time_stop = CurrentTime()
    CalcTime(time_start,time_stop,"Final_Drivers table is ready. Executed Time (sec): ")
    
    # Combining Times, Drivers and calc hours per activity
    time_start = CurrentTime()
    Time_Value = rmf.TimeValues(directory,csv_inputs_f,most_f,Final_Drivers)
    Time_Value = rmf.HoursCalculation(directory,csv_inputs_f,Time_Value,REX_ALLOWANCE) # the same table like above but with hours (overwritten)
    time_stop = CurrentTime()
    CalcTime(time_start,time_stop,"Time Values Dataframe is ready. Executed Time (sec): ")
    
    # Summarizing Hours
    rmf.OutputsComparison(directory,Time_Value,act_model_outputs)
    kpi_outputs = what_if.KpiSaveOutputs(directory,Time_Value,act_model_outputs,kpi) # <- KPI Calculator
    outputs_df = outputs_df.merge(kpi_outputs,on=['Country','Store','Dep'],how='inner') # <- KPI Calculator
outputs_df = outputs_df[outputs_df.Dep!='NEW']
file_name = 'Model_Outputs/KPI_Calculator_May06.xlsx'
outputs_df.to_excel(directory / file_name,index=False)