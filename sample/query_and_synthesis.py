# Use ruamel.yaml instead of yaml because yaml doesn't keep the order of the original .yaml file
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

# Query the data

# Add line to provide external file to select which counties to run

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
        # query_pums_local(state=state, county=county, year=year, pumas=pumas,
        #                hh_vars=hh_vars, p_vars=p_vars)
    else:  # All required files for synthesis already exist, so it doesn't recreate those files
        xwalk = pd.read_csv('data/geo_cross_walk' + ending)
        # We still need the number of puma areas later on, to see how many processors we need for multiprocessing
        pumas = xwalk['PUMA'].unique()
        logging.info('seed_households, seed_persons, geo_cross_walk, and control_totals_tract found in /data')

    # query_pums_local(state=state, county=county, year=year,
    #                  pumas=pumas, hh_vars=hh_vars, p_vars=p_vars)

    print('')
    logging.info('Updating configs/settings.yaml')

    # Edit the input and output file names of settings.yaml
    yaml = ruamel.yaml.YAML()
    with open('configs/settings.yaml', 'r') as file:
        data = yaml.load(file)

    # Modify the data
    # data['state'] = '06'
    # data['county'] = '081'
    # data['year'] = 2017

    data['input_table_list'][0]['filename'] = 'seed_households' + ending
    data['input_table_list'][1]['filename'] = 'seed_persons' + ending
    data['input_table_list'][2]['filename'] = 'geo_cross_walk' + ending
    data['input_table_list'][3]['filename'] = 'control_totals_tract' + ending

    data['output_synthetic_population']['households'][
        'filename'] = 'synthetic_households' + ending
    data['output_synthetic_population']['persons'][
        'filename'] = 'synthetic_persons' + ending

    if year >= 2019:
        if 'RELP' in data['output_synthetic_population']['persons']['columns']:
            data['output_synthetic_population']['persons']['columns'][data['output_synthetic_population']['persons']['columns'].index('RELP')] = 'RELSHIPP'

    elif year < 2019:
        if 'RELSHIPP' in data['output_synthetic_population']['persons']['columns']:
            data['output_synthetic_population']['persons']['columns'][data['output_synthetic_population']['persons']['columns'].index('RELSHIPP')] = 'RELP'

    # Save the modified data back to the .yaml file using ruamel.yaml
    with open('configs/settings.yaml', 'w') as file:
        yaml.dump(data, file)

    logging.info('Updating configs_mp/settings.yaml')
    # Edit congis_mp/settings.yaml to change number of processors than can be used for mp
    with open('configs_mp/settings.yaml', 'r') as file:
        data = yaml.load(file)

    cpu_cores = data['num_processes']
    data['multiprocess_steps'][1]['num_processes'] = min(cpu_cores, len(pumas))
    logging.info('Number of processors being used for multiprocessing: ' + str(min(cpu_cores, len(pumas))))

    with open('configs_mp/settings.yaml', 'w') as file:
        yaml.dump(data, file)

    # The name of the output folder of the populationsim model is hard coded
    # Creates a folder called 'output' if it doesn't already exist
    # The output folder name for this iteration of the loop is not changeable
    if not os.path.exists('output'):
        # Create the folder
        os.makedirs('output')
        logging.info('output folder created')
        time.sleep(2)

    # all_outputs is where all the important files we generate will be stored
    if not os.path.exists('all_outputs'):
        # Create the folder
        os.makedirs('all_outputs')
        logging.info('all_outputs folder created')
        time.sleep(2)

    if ((not os.path.exists('all_outputs/final_summary_TRACT' + ending)) or
            (not os.path.exists('all_outputs/synthetic_households' + ending)) or
            (not os.path.exists('all_outputs/synthetic_persons' + ending))):
        # runs the command 'python run_populationsim.py -c configs_mp -c configs'
        logging.info('Beginning synthesis for ' + state + ', County = ' + county + ', Year = ' + str(year))
        subprocess.run(['python', 'run_populationsim.py', '-c', 'configs_mp', '-c', 'configs'])
    else:
        logging.info('Skipping synthesis for this county. Synthetic households and persons found in /all_outputs')
        time.sleep(5)
        continue

    time.sleep(2)
    # Need to rename the output summary file so it's unique
    logging.info('Renaming output/final_summary_TRACT.csv to output/final_summary_TRACT_' + ending)
    print('')
    print('')

    # Renames the raw summary file name
    os.rename('output/final_summary_TRACT.csv', 'output/final_summary_TRACT' + ending)

    time.sleep(5)

    # Moves the files we need to the all_outputs folder
    shutil.copy2('output/synthetic_households' + ending, 'all_outputs')
    shutil.copy2('output/synthetic_persons' + ending, 'all_outputs')
    shutil.copy2('output/final_summary_TRACT' + ending, 'all_outputs')

    time.sleep(5)


# Generates the summary file after everything is done running

subprocess.run(['python', 'generate_summary.py'])
logging.info('Summary statistics generated in all_outputs/synthesis_stats')

# shutil.rmtree('output') # Doesn't work because of permission errors. I guess this is not a big deal anyways tho
