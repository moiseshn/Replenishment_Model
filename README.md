# Replenishment_Model
The below description is temporary. I will prepare whole pack which helps to understand what my model can do, soon. 

Please find more detailed description of this project in my **[portfolio](www.mariuszborycki.com)**

---
## Background
The Replenishment Model was built for the big trade company I have worked, due to two reasons:
1. Check how many hours every Central European (CE) store needed to cover all activities that must be done in commercial departments, such as:
- Dry Grocery
- Health and Beauty
- Beer Wine Spirits
- Pre-packed Dairy
- Pre-packed Deli
- Frozen
- Produce
- General Merchandise
- Newspapers

To calculate proper hours I use Big Data in Product Number level for each store (> 850 stores) within the company.
The required hours depends on many product features, like e.g:
- Replenishment Type (tray pack, unit, hook etc.)
- Product Weigh
- Shelf Capacity
- Sold Units
- Stock Unit
- and much more...

2. Calculate what would be a financial effect for many ideas which might be implemented. Basically it can help to make a proper (more beneficial) decisions.

---
# Scripts Description for chosen (most important) files: 

- **Replenishment Model.py:**<br>
The main script which is used to calculated demand of hours for Central European stores

- **Replenishment_Model_Functions.py:**<br>
Contains all of the basic functions I needed to make any calculations in Product Number/Store level. I import those functions into the main model __**"Replenishment Model"**__

- **OpsDev_Combiner.py:**<br>
Clean/Transform/Combine some separate files to one pack. In here we can see which product is displays in: 
- Tray Pack, 
- Unit, 
- Full Pallet, 
- Hook etc.

- **Planograms_Combiner.py**<br>
Clean/Transform/Combine 4 separate txt. files contain information about shelf capacity in every CE store

- **PalletCapacity.py**<br>
Every product has own equipment capacity (Pallet/Rollcage). In here I create average Pallet Capacity number for Productivity Measurement Group (PMG) which is used for (at least) half a year for all of the calculations I do in the Replenishment Model. The average Pallet Capacity number is calculated based on sold units in Product Number level for chosen period (usualy ~30 weeks) 

- **CaseCapacity.py**<br>
Every product has own case capacity (items in case). In here I've created average Case Capacity number for Productivity Measurement Group (PMG) which is used for (at least) half a year for all of the calculations I do in the Replenishment Model. The average Case Capacity number is calculated based on sold units in Product Number level for chosen period (usualy ~30 weeks) 
