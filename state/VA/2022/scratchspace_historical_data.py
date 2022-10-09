# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""
Historical data to be ingested for a particular state x policy combination
file name should be `historical_data_{state_code}_{primary_compartment}.py`
    where state_code is of form NJ and primary compartment is the tag of the main compartment relevant to the policy

STATE: VA
POLICY:
VERSION:
DATA SOURCE: not all but #https://www.vadoc.virginia.gov/media/1484/vadoc-state-recidivism-report-2020-02.pdf
    and #https://vadoc.virginia.gov/media/1166/vadoc-offender-population-forecasts-2019-2024.pdf
    and #http://www.vcsc.virginia.gov/Man_Min.pdf
DATA QUALITY: great
HIGHEST PRIORITY MISSING DATA: more years of data
ADDITIONAL NOTES: Just for experimentation
"""
import pandas as pd
import numpy as np
from recidiviz.calculator.modeling.population_projection.utils.spark_bq_utils import (
    upload_spark_model_inputs,
)
from recidiviz.calculator.modeling.population_projection.utils.spark_preprocessing_utils import (
    yearly_to_monthly_data
)
# pylint: skip-file


# RAW DATA

# raw data pulled from the VSCS dashboard with the pivot table expanded to include the following columns:
# Offense Group: high level offense category (ASSAULT, ROBBERY, etc.)
# VCC: the offense code
# FiscalYr: the fiscal year in which the offender was sentenced
# ActDisp: sentencing disposition, type 2 indicates County Jail 3 indicates State Prison
# Category: 0, 1, 2, or 9 representing the crime severity (Category 1 and 2 are violent)
# effsent: the effective sentence in months
# Number of Events: number of sentencing events in this group

raw_va_sentence_df = pd.read_csv(
    'recidiviz/calculator/modeling/population_projection/state/VA/VA_data/unprocessed_va_historical_sentences_v2.csv',
    sep='\t'
)
raw_va_sentence_df['crime_type'] = raw_va_sentence_df['Offense Group'].ffill()
raw_va_sentence_df['offense_code'] = raw_va_sentence_df['VCC'].ffill()
raw_va_sentence_df['crime'] = raw_va_sentence_df['Off1VCC'].ffill()
raw_va_sentence_df['judge_id'] = raw_va_sentence_df['JudgeID'].ffill()
raw_va_sentence_df['sentence_type_code'] = raw_va_sentence_df['ActDisp'].ffill()
raw_va_sentence_df['effective_sentence_months'] = raw_va_sentence_df['effsent']
raw_va_sentence_df['fiscal_year'] = raw_va_sentence_df['FiscalYr'].ffill()
raw_va_sentence_df['life_sentence'] = raw_va_sentence_df['EffLif']
raw_va_sentence_df['offense_date'] = raw_va_sentence_df['Off1Date']

raw_va_sentence_df = raw_va_sentence_df[~raw_va_sentence_df['offense_group'].str.contains('Total')]

# convert the ActDisp numerical value into an incarceration type flag
act_disp_dict = {1: 'probation', 2: 'jail', 3: 'prison'}
raw_va_sentence_df['sentence_type'] = raw_va_sentence_df['sentence_type_code'].apply(lambda x: act_disp_dict[x])

# Filter to the supported sentence types
supported_sentence_types = ["jail", "prison"]
raw_va_sentence_df = raw_va_sentence_df[
    raw_va_sentence_df["sentence_type"].isin(supported_sentence_types)
]


# disaggregation_axes = ['crime', 'judge_id', 'crime_type']
disaggregation_axes = ['crime_type']

# temporary filter
raw_va_sentence_df = raw_va_sentence_df[raw_va_sentence_df.effective_sentence_months > 0]

REFERENCE_YEAR = 2020
# set ts to years for now
raw_va_sentence_df['time_step'] = raw_va_sentence_df.fiscal_year - REFERENCE_YEAR


# OUTFLOWS DATA

outflows_data = raw_va_sentence_df.rename({'sentence_type': 'outflow_to'}, axis=1)

outflows_data = outflows_data.groupby(
    ['time_step', 'outflow_to'] + disaggregation_axes,
    as_index=False
).agg({'OffLName': 'count'})
outflows_data['compartment'] = 'pretrial'
outflows_data = outflows_data.rename({'OffLName': 'total_population'}, axis=1)
# convert to monthly
outflows_data = yearly_to_monthly_data(outflows_data)

# TRANSITIONS DATA
transitions_data = raw_va_sentence_df.rename(
    {'sentence_type': 'compartment', 'effective_sentence_months': 'compartment_duration'},
    axis=1
)

# Don't want sentences listed as hundreds of years to skew our model, so we cap sentence length at 50 years
transitions_data.loc[
    transitions_data.compartment_duration > 50 * 12, "compartment_duration"
] = 50 * 12

transitions_data = transitions_data.groupby(
    ['compartment', 'compartment_duration'] + disaggregation_axes,
    as_index=False
).agg({'OffLName': 'count'})

transitions_data['outflow_to'] = 'release'
transitions_data = transitions_data.rename({'OffLName': 'total_population'}, axis=1)
transitions_data.total_population = transitions_data.total_population.astype(float)


# for each sub-simulation, add in trivial transitions data to define release behavior
if disaggregation_axes:
    for subgroup in transitions_data.groupby(disaggregation_axes).count().index:
        if len(disaggregation_axes) == 1:
            disaggregation_data = {disaggregation_axes[0]: subgroup}
        else:
            disaggregation_data = {disaggregation_axes[i]: subgroup[i] for i in range(len(disaggregation_axes))}
        transitions_data = transitions_data.append(
            {
                **disaggregation_data,
                "compartment": "release",
                "compartment_duration": 360,
                "total_population": 1,
                "outflow_to": "release",
            },
            ignore_index=True,
        )
else:
    # if no disaggregation, need to add a placeholder disaggregation axis
    outflows_data['crime_type'] = 'NA'
    transitions_data['crime_type'] = 'NA'
    transitions_data = transitions_data.append(
        {
            "crime_type": "NA",
            "compartment": "release",
            "compartment_duration": 360,
            "total_population": 1,
            "outflow_to": "release",
        },
        ignore_index=True,
    )

# filter out infrequent offenses
counts = outflows_data.groupby('crime_type').sum().total_population
counts = counts[counts >= 50]
outflows_data = outflows_data[outflows_data.crime_type.apply(lambda x: x in counts.index)]

if disaggregation_axes:
    if len(disaggregation_axes) == 1:
        outflows_subgroups = list(outflows_data.groupby(disaggregation_axes).count().index)
        transitions_data = transitions_data[
            transitions_data.apply(lambda x: x[disaggregation_axes][0] in outflows_subgroups, axis=1)
        ]
    else:
        outflows_subgroups = [list(i) for i in outflows_data.groupby(disaggregation_axes).count().index]
        transitions_data = transitions_data[
            transitions_data.apply(lambda x: list(x[disaggregation_axes]) in outflows_subgroups, axis=1)
        ]


# Ensure every subgroup has at least one datapoint for jail and prison
if disaggregation_axes:
    subgroups = transitions_data.groupby(disaggregation_axes).count().index
    for subgroup in subgroups:
        if len(disaggregation_axes) == 1:
            disaggregation_data = {disaggregation_axes[0]: subgroup}
        else:
            disaggregation_data = {disaggregation_axes[i]: subgroup[i] for i in range(len(disaggregation_axes))}

        compartments = transitions_data[
            (transitions_data[disaggregation_axes] == pd.Series(disaggregation_data)).all(axis=1)
        ].groupby(
            disaggregation_axes + ['compartment'],
            as_index=False
        ).count()[disaggregation_axes + ['compartment']]

        if 'jail' not in compartments.compartment.unique():
            transitions_data = transitions_data.append(
                {
                    **disaggregation_data,
                    "compartment_duration": 1,
                    "compartment": "jail",
                    "outflow_to": "release",
                    "total_population": 1
                },
                ignore_index=True,
            )

        if 'prison' not in compartments.compartment.unique():
            transitions_data = transitions_data.append(
                {
                    **disaggregation_data,
                    "compartment_duration": 0,
                    "compartment": "prison",
                    "outflow_to": "release",
                    "total_population": 1
                },
                ignore_index=True,
            )


# TODO: dedup
# TODO: life sentences
# TODO: add infra to support then add back in 0 length sentences


# STORE DATA
upload_spark_model_inputs(
    "recidiviz-staging",
    "VA_2022_testing",
    outflows_data,
    transitions_data,
    pd.DataFrame(),
    "recidiviz/calculator/modeling/population_projection/state/VA/2022/VA_2022_testing_model_inputs.yaml",
)
