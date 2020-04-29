# Replenishment_Model
The below description is temporary. I will prepare whole pack which helps to understand what my model can do, soon. 

# Background
The Model was built for the trade company where I work, due to two reasons:
1. Check how many hours every Central European (CE) stores need to cover all activities that must be done in commercial departments, such as:
- Dry Grocery
- Health and Beauty
- Beer Wine Spirits
- Pre-packed Dairy
- Pre-packed Deli
- Frozen
- Produce
- General Merchandise
- Newspapers

To calculate proper hours I use data in Product Number (TPN) level for each store (> 850 stores) in the company.
The required hours depends on many product features, like e.g:
- Replenishment Type (tray pack, unit, hook etc.)
- Product Weigh
- Shelf Capacity
- Sold Units
- Stock Unit
- and much more...

2. Calculate what would be a financial effect for many ideas which might be implemented. Basically it can help to make a proper (more beneficial) decisions.

# Scripts Description 
## Replenishment Model.py:
The main script which is used to calculated demand of hours for Central European stores

## Replenishment_Model_Functions.py:
Contains all of the basic functions I need to make any calculations in TPN/Store level. I import those functions into the main model "Replenishment Model"

## OpsDev_Combiner.py:
Clean/Transform/Combine some separate files to one pack. In here we can see which product is displays in: 
- Tray Pack, 
- Unit, 
- Full Pallet, 
- Hook etc.

## Planograms_Combiner.py
Clean/Transform/Combine 4 separate txt. files contain information about shelf capacity in every CE store

## PalletCapacity_Customizer.py
Every product has own equipment capacity (Pallet/Rollcage). In here I create average Pallet Capacity number for Productivity Measurement Group (PMG) which is used for (at least) half a year for all of the calculations I do in the Replenishment Model. The average Pallet Capacity number is calculated based on sold units in TPN level for chosen period (usualy ~30 weeks) 
