# Spark Population Projection

## What is Spark
The Spark model is designed to simulate the criminal justice system in 
order to forecast populations and predict the impact of different policies. 
The model is designed as a stock and flow simulation that uses historical 
data to calculate how people flow through the criminal justice system. 
Then, when policies are applied to the simulation the flow changes 
and the estimated population difference can be measured, and 
the cost differential can be computed.

## Where things live
**Config Files**

These are yaml files detailing the inputs for state-specific projections. These live in `./state/` (inside state-specific folders)

**Modeling**

The classes and methods used for modeling are in the main directory and are often accessed through Jupyter notebooks, such as the ones in `./notebooks/`

## Other Resources

The Spark Google Drive folder contains a lot of additional resources including:
- More detailed Spark methodology
- Data pre-processing onboarding materials
- Output from data pre-processing

## Port of Recidiviz code

This is basically just the entire recidiviz.calculator.population_projection.modeling directory copied over from [pulse-data](https://github.com/Recidiviz/pulse-data). There are a few specifics of the move worth mentioning:
* We took recidiviz.utils.yaml_dict and moved it into our utils folder
* We didn't take the microsimulations folder or any of the Recidiviz notebooks
