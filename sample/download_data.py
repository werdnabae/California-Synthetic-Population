import pandas as pd
import ruamel.yaml
import subprocess
import os
import logging
# This is a new file Andrew created to query the data
from county_query import query_acs, query_pums_local, query_pums_API
import shutil
import time
import argparse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# These are the three variables be changed.

# state = '06'  # FIPS code for State
# # county = '' # This is set inside the for loop
# year = 2017  # Year as an int

hh_vars = ['WGTP', 'NP', 'HINCP', 'NOC', 'ADJINC', 'TEN', 'SERIALNO', 'PUMA']
p_vars = ['PWGTP', 'AGEP', 'RAC1P', 'SEX', 'RELP', 'ESR', 'SERIALNO', 'ADJINC',
          'PINCP', 'PUMA']

bay_area = ['001', '013', '041', '055', '075', '081', '085', '095', '097']

parser = argparse.ArgumentParser()
parser.add_argument("--state", type=str)
parser.add_argument("--year", type=int)
parser.add_argument("--counties", type=str)
args = parser.parse_args()

state = args.state
year = args.year
if args.counties == 'bay_area':
    counties = bay_area
elif args.counties == 'all_CA':
    counties = ["{:03d}".format(num) for num in range(1, 116) if num % 2 != 0]
else:
    counties = args.counties

# In 2019, the 'RELP' variable was renamed to 'RELSHIPP'
if year >= 2019 and 'RELP' in p_vars:
    p_vars = ['RELSHIPP' if item == 'RELP' else item for item in p_vars]

for county in counties:
    # for county in ['013']:
    # query_acs returns the unique PUMA codes that are in the queried county,

    # as they are needed to query the PUMS data
    logging.info('Beginning query and synthesis for ' + state + ', County = ' + county + ', Year = ' + str(year))

    if not os.path.exists('data'):  # this data folder can be changed in
        # Create the folder
        os.makedirs('data')
        logging.info('data folder created')

    ending = ('_' + state + '_' + county + '_' + str(year) + '.csv')

    # If the geo_cross_walk, control totals, household and persons seed data does not already exist in the data folder, create those files
    if ((not os.path.exists('data/seed_households' + ending)) or
            (not os.path.exists('data/seed_persons' + ending)) or
            (not os.path.exists('data/geo_cross_walk' + ending)) or
            (not os.path.exists('data/control_totals_tract' + ending))):
        pumas = query_acs(state=state, county=county, year=year)
        query_pums_API(state=state, county=county, year=year, pumas=pumas,
                       hh_vars=hh_vars, p_vars=p_vars)
        #query_pums_local(state=state, county=county, year=year, pumas=pumas,
        #               hh_vars=hh_vars, p_vars=p_vars)
        time.sleep(5)
    else:
        logging.info('seed_households, seed_persons, geo_cross_walk, and control_totals_tract found in /data')

    # query_pums_local(state=state, county=county, year=year,
    #                  pumas=pumas, hh_vars=hh_vars, p_vars=p_vars)

    print('')
