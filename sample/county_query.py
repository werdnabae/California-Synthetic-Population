# from synthpop.census_helpers import Census
import pandas as pd
import requests
import numpy as np
import census
import os
import logging
from typing import List
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class Census:
    '''
    Class created to access the census API. This is only needed for the ACS marginal data.
    All of these functions were taken from the SynthPop code, but it no longer requires anything from the synthpop package or anything from their AWS s3 server
    '''

    def __init__(self, key):
        #  A key is required to access the census API. You can just use the same key in the query_acs function.
        self.c = census.Census(key)

    def tract_query(self, census_columns, state, county, year=2016, tract=None):
        '''
        This function is called when you want to get the marginals at the tract level.

        Parameters
        ----------
        census_columns
        state
        county
        year
        tract

        Returns
        -------
        Marginals at the tract level for the census_columns
        '''

        if tract is None:  # If tract is not given as a parameter, select all tracts
            tract = "*"
        return self._query(census_columns, state, county,
                           forstr="tract:%s" % tract,
                           year=year)

    def _query(self, census_columns, state, county, forstr, year, tract=None):
        '''
        This function is the one that actually calls the census API

        Parameters
        ----------
        census_columns
        state
        county
        forstr
        year
        tract

        Returns
        -------
        Marginals for census_columns for whatever geographic level 'forstr' is
        '''
        c = self.c

        if tract is None:
            in_str = 'state:%s county:%s' % (state, county)
        else:
            in_str = 'state:%s county:%s tract:%s' % (state, county, tract)

        dfs = []

        # unfortunately the api only queries 50 columns at a time (comment from Synthpop developers)
        # However, Andrew thinks that the code probably still works if you do more than 50 at a time
        # leave room for a few extra id columns
        def chunks(l, n):
            """ Yield successive n-sized chunks from l.
            """
            for i in range(0, len(l), n):
                yield l[i:i + n]

        for census_column_batch in chunks(census_columns, 45):
            census_column_batch = list(census_column_batch)
            d = c.acs5.get(['NAME'] + census_column_batch,
                           geo={'for': forstr,
                                'in': in_str}, year=year)
            df = pd.DataFrame(d)
            df[census_column_batch] = df[census_column_batch].astype('int')
            dfs.append(df)

        assert len(dfs) >= 1
        df = dfs[0]
        for mdf in dfs[1:]:
            df = pd.merge(df, mdf, on="NAME", suffixes=("", "_ignore"))
            drop_cols = list(filter(lambda x: "_ignore" in x, df.columns))
            df = df.drop(drop_cols, axis=1)

        return df

    def _scale_and_merge(self, df1, tot1, df2, tot2, columns_to_scale,
                         merge_columns, suffixes):
        '''
        (Not utilized)
        This is a helper function from the Synthpop code, when you want get have
        some variables at the tract level and other variables at the block group level

        Parameters
        ----------
        df1
        tot1
        df2
        tot2
        columns_to_scale
        merge_columns
        suffixes

        Returns
        -------

        '''
        # df1 is the disaggregate data frame (e.g. block groups)
        # df2 is the aggregate data frame (e.g. tracts)
        # need to scale down df2 variables to df1 level totals
        df = pd.merge(df1, df2, left_on=merge_columns, right_on=merge_columns,
                      suffixes=suffixes)

        # going to scale these too so store current values
        tot2, tot1 = df[tot2], df[tot1]
        # if agg number if 0, disaggregate should be 0
        # note this is filled by fillna below
        assert np.all(tot1[tot2 == 0] == 0)

        for col in columns_to_scale:
            df[col] = df[col] / tot2 * tot1
            # round?
            df[col] = df[col].fillna(0).astype('int')
        return df

    def block_group_query(self, census_columns, state, county, year=2016,
                          tract=None, id=None):
        '''
        (Not utilized). Leftover form Synthpop code
        This the function called when you want to get marginals at the block group level

        Parameters
        ----------
        census_columns
        state
        county
        year
        tract
        id

        Returns
        -------

        '''
        if id is None:
            id = "*"
        return self._query(census_columns, state, county,
                           forstr="block group:%s" % id,
                           tract=tract, year=year)

    def block_group_and_tract_query(self, block_group_columns,
                                    tract_columns, state, county,
                                    merge_columns, block_group_size_attr,
                                    tract_size_attr, year=2016, tract=None):
        '''
        (Not utilized). Leftover from Synthpop code
        This is a function used when you want some columns at the block group level and others at the tract level

        Parameters
        ----------
        block_group_columns
        tract_columns
        state
        county
        merge_columns
        block_group_size_attr
        tract_size_attr
        year
        tract

        Returns
        -------

        '''
        df2 = self.tract_query(tract_columns, state, county, tract=tract,
                               year=year)
        df1 = self.block_group_query(block_group_columns, state, county,
                                     tract=tract, year=year)

        df = self._scale_and_merge(df1, block_group_size_attr, df2,
                                   tract_size_attr, tract_columns,
                                   merge_columns, suffixes=("", "_ignore"))
        drop_cols = list(filter(lambda x: "_ignore" in x, df.columns))
        df = df.drop(drop_cols, axis=1)

        return df


def query_acs(state: str, county: str, year: int):
    state = state  # FIPS code for the state
    county = county  # FIPS code for the county
    year = year  # ACS year you want to query from as an int

    logging.info('Downloading ACS Marginals for: State = ' +
                 state + ', County = ' + county + ', Year = ' + str(year))

    # this a key needed to access te Census API. You can use this same one
    c = Census("285e70bec59918991430e98163a4cda2802b4f01")

    logging.info('Downloading raw household marginals...')
    # Extract the columns we need for households
    # Column names: https://api.census.gov/data/2018/acs/acs5/variables.html
    income_columns = ['B19001_0%02dE' % i for i in range(1, 18)]
    hhsize_columns = ['B11016_0%02dE' % i for i in range(1, 17)]
    hhrace_columns = ['B25006_0%02dE' % i for i in range(1, 11)]
    hhown_columns = ['B25003_0%02dE' % i for i in range(1, 4)]
    work_columns = ['B08202_0%02dE' % i for i in range(1, 6)]
    child_column = ['B11005_002E']
    tract_columns = (income_columns + hhsize_columns + hhrace_columns +
                     hhown_columns + work_columns + child_column)

    # Downloads the ACS totals at the Census Tract level
    h_acs = c.tract_query(tract_columns, state=state, county=county,
                          year=year)
    logging.info('Raw household marginal download complete!')
    time.sleep(2)
    # Extract the columns we need for people
    # Column names: https://api.census.gov/data/2018/acs/acs5/variables.html

    logging.info('Downloading person marginals...')
    population = ['B01001_001E']
    sex = ['B01001_002E', 'B01001_026E']
    race = ['B02001_0%02dE' % i for i in range(1, 11)]
    male_age_columns = ['B01001_0%02dE' % i for i in range(3, 26)]
    female_age_columns = ['B01001_0%02dE' % i for i in range(27, 50)]
    all_columns = population + sex + race + male_age_columns + female_age_columns
    p_acs = c.tract_query(all_columns, state=state, county=county, year=year)
    logging.info('Person marginal download complete!')
    time.sleep(2)
    # Create geo_cross_walk.csv.
    # Basically this shows the relationship between each tract and its PUMA code

    # Take a look at this:
    # http://proximityone.com/acs1620pums.htm#:~:text=PUMA%20Vintages,-ACS%205%2Dyear&text=The%20ACS%202020%205%2Dyear,uses%20the%202010%20vintage%20PUMAs.

    # 2020 Census Tract to 2020 PUMA Relationship file and the 2010 Census Tract to 2010 PUMA relationship file
    # can be downloaded here: (they are also both in the raw_data folder)
    # https://www.census.gov/programs-surveys/geography/guidance/geo-areas/pumas.html

    if year >= 2022:
        # ACS uses 2020 census tracts, and PUMS uses 2020 PUMA codes
        # Need to know relationship from 2020 census tract -> 2020 PUMA code

        raw_xwalk_path = '../raw_data/tract_to_puma/2020_Census_Tract_to_2020_PUMA.txt'
        tracts = h_acs['tract']

    elif year == 2021 or year == 2020:
        # ACS uses 2020 census tracts, but PUMS still uses 2010 PUMA codes
        # Need to know relationship from 2020 census tract -> 2010 census tract -> 2010 PUMA code
        # The 2020 Tract to 2010 Tract relationship file can be downloaded here (it's also already in the raw_data folder):
        # https://www.census.gov/geographies/reference-files/time-series/geo/relationship-files.2020.html#list-tab-1709067297

        raw_xwalk_path = '../raw_data/tract_to_puma/2010_Census_Tract_to_2010_PUMA.txt'
        tracts = []
        tract_relationship_path = '../raw_data/tract_to_puma/2020_Census_Tract_to_2010_Census_Tract.txt'
        tract_relationship = pd.read_csv(tract_relationship_path, delimiter='|',
                                         dtype={'GEOID_TRACT_20': str, 'GEOID_TRACT_10': str})
        old_counties = []
        for tract in h_acs['tract']:
            geoid_tract_20 = state + county + str(tract)
            old_counties.append(int(tract_relationship[tract_relationship['GEOID_TRACT_20'] == geoid_tract_20].iloc[0]['GEOID_TRACT_10'][2:5]))
            tracts.append(int(tract_relationship[tract_relationship['GEOID_TRACT_20'] == geoid_tract_20].iloc[0]['GEOID_TRACT_10'][5:]))
            # if int(tract_relationship[tract_relationship['GEOID_TRACT_20'] == geoid_tract_20].iloc[0]['GEOID_TRACT_10'][5:]) == 3306:
            #     print(tract_relationship[tract_relationship['GEOID_TRACT_20'] == geoid_tract_20].iloc[0])

    else:  # If year is 2019 or earlier
        # ACS uses 2010 census tracts, and PUMS uses 2010 PUMA codes
        # Need to know relationship from 2010 census tract -> 2010 PUMA codes

        raw_xwalk_path = '../raw_data/tract_to_puma/2010_Census_Tract_to_2010_PUMA.txt'
        tracts = h_acs['tract']

    raw_xwalk = pd.read_csv(raw_xwalk_path, delimiter=',')

    logging.info('Creating geo_cross_walk file...')
    # %%
    column_names = ['TRACT', 'PUMA', 'REGION']
    geo_cross_walk = pd.DataFrame(columns=column_names)
    # for tract in tracts:
    puma_codes_unique = []
    puma_codes_all = []

    for i, tract in enumerate(tracts):
        # print(tract)
        tract = int(tract)
        if tract == 137000 and year < 2020:
            new_row = {'TRACT': 137000, 'PUMA': '03725', 'REGION': 1}
        elif year <= 2019 or year >= 2022:
            puma_code = raw_xwalk[(raw_xwalk['STATEFP'] == int(state)) &
                                  (raw_xwalk['COUNTYFP'] == int(county)) &
                                  (raw_xwalk['TRACTCE'] == int(tract))][
                'PUMA5CE'].item()
            new_row = {'TRACT': tract, 'PUMA': puma_code, 'REGION': 1}
        elif year == 2020 or year == 2021:
            puma_code = raw_xwalk[(raw_xwalk['STATEFP'] == int(state)) &
                                  (raw_xwalk['COUNTYFP'] == int(old_counties[i])) &
                                  (raw_xwalk['TRACTCE'] == int(tract))][
                'PUMA5CE'].item()
            new_row = {'TRACT': h_acs['tract'][i], 'PUMA': puma_code, 'REGION': 1}

        new_row_df = pd.DataFrame(new_row, index=[0])
        geo_cross_walk = pd.concat([geo_cross_walk, new_row_df],
                                   ignore_index=True)

        if puma_code not in puma_codes_unique:
            puma_codes_unique.append(puma_code)
        puma_codes_all.append(int(puma_code))

    # puma_codes_unique = []
    # puma_codes_all = []
    #
    # for tract in tracts:
    #     # code = c.tract_to_puma(state, county, tract)
    #     code = raw_xwalk[(raw_xwalk['STATEFP'] == int(state)) &
    #                      (raw_xwalk['COUNTYFP'] == int(county)) &
    #                      (raw_xwalk['TRACTCE'] == int(tract))]['PUMA5CE'].item()
    #     if code not in puma_codes_unique:
    #         puma_codes_unique.append(code)
    #     puma_codes_all.append(int(code))
    h_acs['PUMA'] = puma_codes_all

    crosswallk_name = ('data/geo_cross_walk_' + state + '_' + county +
                       '_' + str(year) + '.csv')
    geo_cross_walk.to_csv(crosswallk_name, index=False)
    logging.info('Geo_cross_walk file saved as ' + crosswallk_name)


    #  Categorize the household table into the categories that we want
    logging.info('Categorizing households')

    h_acs['num_hh'] = h_acs['B19001_001E']
    h_acs['children_yes'] = h_acs['B11005_002E']
    h_acs['children_no'] = h_acs['B19001_001E'] - h_acs['B11005_002E']

    h_acs['income0'] = h_acs['B19001_002E'] + h_acs['B19001_003E']
    h_acs['income1'] = h_acs['B19001_004E'] + h_acs['B19001_005E']
    h_acs['income2'] = h_acs['B19001_006E'] + h_acs['B19001_007E'] + h_acs[
        'B19001_008E'] + h_acs['B19001_009E'] + h_acs['B19001_010E']
    h_acs['income3'] = h_acs['B19001_011E'] + h_acs['B19001_012E']
    h_acs['income4'] = h_acs['B19001_013E']
    h_acs['income5'] = h_acs['B19001_014E'] + h_acs['B19001_015E']
    h_acs['income6'] = h_acs['B19001_016E']
    h_acs['income7'] = h_acs['B19001_017E']

    h_acs['size1'] = h_acs['B11016_010E']
    h_acs['size2'] = h_acs['B11016_003E'] + h_acs['B11016_011E']
    h_acs['size3'] = h_acs['B11016_004E'] + h_acs['B11016_012E']
    h_acs['size4'] = h_acs['B11016_005E'] + h_acs['B11016_013E']
    h_acs['size5'] = h_acs['B11016_006E'] + h_acs['B11016_014E']
    h_acs['size6'] = h_acs['B11016_007E'] + h_acs['B11016_015E']
    h_acs['size7'] = h_acs['B11016_008E'] + h_acs['B11016_016E']

    h_acs['work0'] = h_acs['B08202_002E']
    h_acs['work1'] = h_acs['B08202_003E']
    h_acs['work2'] = h_acs['B08202_004E']
    h_acs['work3'] = h_acs['B08202_005E']

    h_acs['asian'] = h_acs['B25006_005E']
    h_acs['not_asian'] = h_acs['B19001_001E'] - h_acs['B25006_005E']

    h_acs['hh_own'] = h_acs['B25003_002E']
    h_acs['hh_rent'] = h_acs['B25003_003E']
    h_acs['TRACT'] = h_acs['tract']
    h_acs['STATE'] = h_acs['state']
    h_acs['COUNTY'] = h_acs['county']

    # Specify the desired order of columns
    col_order = [
        'STATE',
        'COUNTY',
        'TRACT',
        'PUMA',
        'num_hh',
        'children_yes',
        'children_no',
        'income0',
        'income1',
        'income2',
        'income3',
        'income4',
        'income5',
        'income6',
        'income7',
        'size1',
        'size2',
        'size3',
        'size4',
        'size5',
        'size6',
        'size7',
        'hh_own',
        'hh_rent',
        'work0',
        'work1',
        'work2',
        'work3',
        'asian',
        'not_asian'
    ]
    h_acs_cat = h_acs.reindex(columns=col_order)
    logging.info('Categorizing households complete!')

    # Categorize the persons table into the categories that we want

    logging.info('Categorizing persons')
    p_acs['num_persons'] = p_acs['B01001_001E']
    p_acs['age_lt19'] = (
            p_acs['B01001_003E'] + p_acs['B01001_004E'] + p_acs['B01001_005E'] +
            p_acs['B01001_006E'] + p_acs['B01001_007E'] +
            p_acs['B01001_027E'] + p_acs['B01001_028E'] + p_acs['B01001_029E'] +
            p_acs['B01001_030E'] + p_acs['B01001_031E'])
    p_acs['age_20-35'] = (
            p_acs['B01001_008E'] + p_acs['B01001_009E'] + p_acs['B01001_010E'] +
            p_acs['B01001_011E'] + p_acs['B01001_012E'] +
            p_acs['B01001_032E'] + p_acs['B01001_033E'] + p_acs['B01001_034E'] +
            p_acs['B01001_035E'] + p_acs['B01001_036E'])
    p_acs['age_35-60'] = (
            p_acs['B01001_013E'] + p_acs['B01001_014E'] + p_acs['B01001_015E'] +
            p_acs['B01001_016E'] + p_acs['B01001_017E'] +
            p_acs['B01001_037E'] + p_acs['B01001_038E'] + p_acs['B01001_039E'] +
            p_acs['B01001_040E'] + p_acs['B01001_041E'])
    p_acs['age_60+'] = (
            p_acs['B01001_018E'] + p_acs['B01001_019E'] + p_acs['B01001_020E'] +
            p_acs['B01001_021E'] + p_acs['B01001_022E'] +
            p_acs['B01001_023E'] + p_acs['B01001_024E'] + p_acs['B01001_025E'] +
            p_acs['B01001_042E'] + p_acs['B01001_043E'] +
            p_acs['B01001_044E'] + p_acs['B01001_045E'] + p_acs['B01001_046E'] +
            p_acs['B01001_047E'] + p_acs['B01001_048E'] +
            p_acs['B01001_049E'])
    p_acs['race_white'] = p_acs['B02001_002E']
    p_acs['race_black'] = p_acs['B02001_003E']
    p_acs['race_asian'] = p_acs['B02001_005E']
    p_acs['race_other'] = p_acs['B02001_004E'] + p_acs['B02001_006E'] + p_acs[
        'B02001_007E'] + p_acs['B02001_008E']
    p_acs['sex_male'] = p_acs['B01001_002E']
    p_acs['sex_female'] = p_acs['B01001_026E']

    col_order = ['num_persons',
                 'age_lt19',
                 'age_20-35',
                 'age_35-60',
                 'age_60+',
                 'race_white',
                 'race_black',
                 'race_asian',
                 'race_other',
                 'sex_male',
                 'sex_female']

    p_acs_cat = p_acs.reindex(columns=col_order)
    logging.info('Categorizing persons complete!')

    # Save the household and persons controls as one file
    all_totals = pd.concat([h_acs_cat, p_acs_cat], axis=1, join='outer')
    save_file_name = 'data/control_totals_tract_' + state + '_' + county + '_' + str(
        year) + '.csv'
    all_totals.to_csv(save_file_name, index=False)
    logging.info('Saved household and persons controls as: ' + save_file_name)
    time.sleep(2)
    print('')

    # Returns the unique PUMA codes, because that is needed to download/organize the PUMS data
    return puma_codes_unique


def query_pums_API(state: str, county: str, year: str, pumas: list, hh_vars: list, p_vars: list):
    """
    Queries the PUMS data through the PUMS API
    Basically you select your variables and geographies and use the API get query from here:
    https://data.census.gov/mdat/#/search?ds=ACSPUMS5Y2017

    Parameters
    ----------
    state (str): FIPS code
    county (str): FIPS code
    year (int): PUMS year to use
    pumas (list): List of PUMA codes for this county, returned from the ACS query

    Returns
    -------
    Nothing
    """

    # The PUMA codes need to be strs for this function
    pumas = [str(puma) for puma in pumas]

    # Query raw household PUMS data.

    logging.info('Downloading raw household PUMS data...')
    url = 'https://api.census.gov/data/' + str(year) + '/acs/acs5/pums'

    # UCGID stands for Unique Confidential Geographic Identifier
    # It is used the PUMS API to download each PUMA
    ucgid_base = '7950000US' + state  # all of them start with this
    ucgid = []

    for puma in pumas:
        # PUMA code is 5 digits, so fill the beginning if 0 if needed
        ucgid.append(ucgid_base + puma.zfill(5))

    # 'get' are the PUMS variables to download. Sometimes the variables
    # change, so check to the data dictionary for the year you are using
    # https://www2.census.gov/programs-surveys/acs/tech_docs/pums/data_dict
    # /PUMS_Data_Dictionary_2017.pdf
    params = {
        "get": ','.join(hh_vars),  #"WGTP,NP,HINCP,NOC,ADJINC,TEN,SERIALNO",
        'ucgid': ','.join(ucgid)
        # ucgid should now look something like this:
        # "7950000US0608101,7950000US0608102,7950000US0608103,7950000US0608104,7950000US0608105,7950000US0608106"
    }

    response = requests.get(url, params=params)
    if response.status_code == 200:
        hh_PUMS = pd.DataFrame(response.json()[1:], columns=response.json()[0])
        logging.info('Raw household PUMS downloaded successfuly!')
        # hh_PUMS.to_csv(('data/raw_households_' + state + '_' + county + '_' + str(year) + '.csv'))
    else:
        logging.error("Error occurred while downloading household PUMS data:",
                      response.status_code)
    time.sleep(2)
    logging.info('Downloading raw persons PUMS data...')
    # Query raw persons PUMS data.
    # We use the same url and same ucgid as the household PUMS data

    # Make sure to check the PUMS variables to make sure that they exist for that year
    # For example, RELP was changed to RELSHIPP for 2019
    params = {
        "get": ','.join(p_vars),   #"PWGTP,WGTP,AGEP,RAC1P,SEX,RELP,ESR,SERIALNO,ADJINC,PINCP",
        'ucgid': ','.join(ucgid)
    }

    response = requests.get(url, params=params)
    if response.status_code == 200:
        # If the request was successful, save the response content to a file
        p_PUMS = pd.DataFrame(response.json()[1:], columns=response.json()[0])
        logging.info('Raw persons PUMS downloaded successfuly!')
        # p_PUMS.to_csv(('data/raw_persons_' + state + '_' + county + '_' + str(year) + '.csv'))
    else:
        logging.error("Error occurred while downloading persons PUMS data:",
                      response.status_code)

    # Remap the SERIALNO, so that it's from 1 to the number of households
    serialno_mapping = dict(zip(hh_PUMS['SERIALNO'], range(len(hh_PUMS))))
    hh_PUMS['SERIALNO'] = hh_PUMS['SERIALNO'].map(serialno_mapping)
    p_PUMS['SERIALNO'] = p_PUMS['SERIALNO'].map(serialno_mapping)

    # Change all values of the dataframe into an int or float
    hh_PUMS = hh_PUMS.apply(pd.to_numeric, errors="coerce")
    p_PUMS = p_PUMS.apply(pd.to_numeric, errors="coerce")
    time.sleep(2)
    # Remove zero-weight households and persons

    # According to the PUMS variable documentation:
    # https://www2.census.gov/programs-surveys/acs/tech_docs/pums/data_dict/PUMS_Data_Dictionary_2017.pdf
    # WGTP == 0 means that it is a group quarters housing place
    # NP == 0 means that this is a vacant unit
    empty = list(
        hh_PUMS[(hh_PUMS['WGTP'] == 0) | (hh_PUMS['NP'] == 0)]['SERIALNO'])
    hh_PUMS = hh_PUMS[~hh_PUMS['SERIALNO'].isin(empty)]
    # also drops the people who are associated with these deleted housing data
    p_PUMS = p_PUMS[~p_PUMS['SERIALNO'].isin(empty)]

    hh_PUMS = hh_PUMS.reset_index(drop=True)
    p_PUMS = p_PUMS.reset_index(drop=True)

    # Following three lines ensure that there are no empty households in the PUMS data anymore
    assert len(hh_PUMS[hh_PUMS['NP'] == 0]) == 0
    assert len(hh_PUMS[hh_PUMS['TEN'] == 0]) == 0
    assert len(hh_PUMS[hh_PUMS['HINCP'] == -60000]) == 0

    # Remove duplicate serial numbers (there shouldn't be any, but just in case)
    logging.info('Removing duplicate serial numbers (there should not be any')
    # %%
    duplicates = hh_PUMS[hh_PUMS['SERIALNO'].duplicated()]['SERIALNO']
    for i in range(len(duplicates)):
        hh_PUMS = hh_PUMS[hh_PUMS['SERIALNO'] != duplicates.iloc[i]]
        p_PUMS = p_PUMS[p_PUMS['SERIALNO'] != duplicates.iloc[i]]

    # Renumber the SERIALNO to go from 0 to (# of households)
    logging.info('Remapping SERIALNO')
    serialno_mapping = dict(zip(hh_PUMS['SERIALNO'], range(len(hh_PUMS))))
    hh_PUMS['SERIALNO'] = hh_PUMS['SERIALNO'].map(serialno_mapping)
    p_PUMS['SERIALNO'] = p_PUMS['SERIALNO'].map(serialno_mapping)

    # sometimes the p_PUMS are not sorted by SERIALNO after the previous operation
    p_PUMS = p_PUMS.sort_values('SERIALNO')

    # Add a household race column
    logging.info('Adding household race column')
    hh_race = []
    for i in range(len(p_PUMS)):
        if year < 2019:
            if p_PUMS['RELP'][i] == 0:
                hh_race.append(p_PUMS['RAC1P'][i])
        elif year >= 2019:
            if p_PUMS['RELSHIPP'][i] == 20:
                hh_race.append(p_PUMS['RAC1P'][i])

    hh_PUMS['HEAD_RACE'] = hh_race

    # Add a column for the number of adults in a household
    logging.info('Adding column for number of adults in a household')

    # Below is old code
    # adult_count = []
    # worker_count = []
    # serialno = 0
    # adults = 0
    # workers = 0
    # for i, row in p_PUMS.iterrows():
    #     if p_PUMS['SERIALNO'][i] != serialno:
    #         adult_count.append(adults)
    #         worker_count.append(workers)
    #         adults = 0
    #         workers = 0
    #         serialno = p_PUMS['SERIALNO'][i]
    #     if p_PUMS['AGEP'][i] >= 18:
    #         adults += 1
    #     if p_PUMS['ESR'][i] in [1, 2, 4, 5]:
    #         workers += 1
    # adult_count.append(adults)
    # worker_count.append(workers)
    # hh_PUMS['NUM_ADULTS'] = adult_count
    # hh_PUMS['NUM_WORKERS'] = worker_count

    # Below is new code I got from ChatGPT
    grouped_data = p_PUMS.groupby('SERIALNO').agg(
        NUM_ADULTS=pd.NamedAgg(column='AGEP', aggfunc=lambda x: (x >= 18).sum()),
        NUM_WORKERS=pd.NamedAgg(column='ESR', aggfunc=lambda x: x.isin([1, 2, 4, 5]).sum())
    )

    # Merge the aggregated data back to hh_PUMS based on 'SERIALNO'
    hh_PUMS = pd.merge(hh_PUMS, grouped_data, left_on='SERIALNO', right_index=True)

    # Saves the hh_PUMS and p_PUMS dataframes to .csv files
    hh_PUMS_save_name = ('data/seed_households_' + state + '_' + county +
                         '_' + str(year) + '.csv')
    p_PUMS_save_name = ('data/seed_persons_' + state + '_' + county +
                        '_' + str(year) + '.csv')
    hh_PUMS.to_csv(hh_PUMS_save_name, index=False)
    p_PUMS.to_csv(p_PUMS_save_name, index=False)
    logging.info('Seed households saved to: ' + hh_PUMS_save_name)
    time.sleep(2)
    logging.info('Seed persons saved to: ' + p_PUMS_save_name)
    time.sleep(2)
    print('')


def query_pums_local(state: str, county: str, year: str, pumas: List[int],
                     hh_vars: List[str], p_vars: List[str]):
    """
    This queries the PUMS data using files that you have already downloaded on your computer
    For example, from here: https://www2.census.gov/programs-surveys/acs/data/pums/2017/5-Year/
    Andrew used the 5-year PUMS data
    The reason for this is that you don't have to use the API for each county.
    We didn't run into this problem for PUMS, but for ACS, sometimes the connection to the API would not be good

    Parameters
    ----------
    state
    county
    year
    pumas
    hh_vars
    p_vars

    Returns
    -------

    """
    # The PUMA codes need to be integers for querying using the local file
    pumas = [int(puma) for puma in pumas]

    # Where the raw PUMS data is located
    raw_hh_loc = '../raw_data/pums_raw/'
    raw_p_loc = '../raw_data/pums_raw/'

    # the file should be named like 'hh_pums_raw_2017.csv' (replace 2017 for the actual year)

    hh_PUMS = pd.read_csv(raw_hh_loc + 'hh_pums_raw_' + str(state) + '_' + str(year) + '.csv')
    hh_PUMS = hh_PUMS[hh_PUMS['PUMA'].isin(pumas)]
    hh_PUMS = hh_PUMS.loc[:, hh_vars]

    p_PUMS = pd.read_csv(raw_hh_loc + 'p_pums_raw_' + str(state) + '_' + str(year) + '.csv')
    p_PUMS = p_PUMS[p_PUMS['PUMA'].isin(pumas)]
    p_PUMS = p_PUMS.loc[:, p_vars]

    # Change all values of the dataframe into an int or float
    hh_PUMS = hh_PUMS.apply(pd.to_numeric, errors="coerce")
    p_PUMS = p_PUMS.apply(pd.to_numeric, errors="coerce")

    # Remove zero-weight households and persons

    # According to the PUMS variable documentation:
    # https://www2.census.gov/programs-surveys/acs/tech_docs/pums/data_dict/PUMS_Data_Dictionary_2017.pdf
    # WGTP == 0 means that it is a group quarters housing place
    # NP == 0 means that this is a vacant unit
    empty = list(
        hh_PUMS[(hh_PUMS['WGTP'] == 0) | (hh_PUMS['NP'] == 0)]['SERIALNO'])
    hh_PUMS = hh_PUMS[~hh_PUMS['SERIALNO'].isin(empty)]
    # also drops the people who are associated with these deleted housing data
    p_PUMS = p_PUMS[~p_PUMS['SERIALNO'].isin(empty)]

    hh_PUMS = hh_PUMS.reset_index(drop=True)
    p_PUMS = p_PUMS.reset_index(drop=True)

    # Following three lines ensure that there are no empty households in the PUMS data anymore
    assert len(hh_PUMS[hh_PUMS['NP'] == 0]) == 0
    assert len(hh_PUMS[hh_PUMS['TEN'] == 0]) == 0
    assert len(hh_PUMS[hh_PUMS['HINCP'] == -60000]) == 0

    # Remove duplicate serial numbers (there shouldn't be any, but just in case)
    logging.info('Removing duplicate serial numbers (there should not be any')
    # %%
    duplicates = hh_PUMS[hh_PUMS['SERIALNO'].duplicated()]['SERIALNO']
    for i in range(len(duplicates)):
        hh_PUMS = hh_PUMS[hh_PUMS['SERIALNO'] != duplicates.iloc[i]]
        p_PUMS = p_PUMS[p_PUMS['SERIALNO'] != duplicates.iloc[i]]

    # Renumber the SERIALNO to go from 0 to (# of households)
    logging.info('Remapping SERIALNO')
    serialno_mapping = dict(zip(hh_PUMS['SERIALNO'], range(len(hh_PUMS))))
    hh_PUMS['SERIALNO'] = hh_PUMS['SERIALNO'].map(serialno_mapping)
    p_PUMS['SERIALNO'] = p_PUMS['SERIALNO'].map(serialno_mapping)

    # sometimes the p_PUMS are not sorted by SERIALNO after the previous operation
    p_PUMS = p_PUMS.sort_values('SERIALNO')

    # Add a household race column
    logging.info('Adding household race column')
    hh_race = []
    for i in range(len(p_PUMS)):
        if year < 2019:
            if p_PUMS['RELP'][i] == 0:
                hh_race.append(p_PUMS['RAC1P'][i])
        elif year >= 2019:
            if p_PUMS['RELSHIPP'][i] == 20:
                hh_race.append(p_PUMS['RAC1P'][i])

    # Add a column for the number of adults in a household
    logging.info('Adding column for number of adults in a household')
    adult_count = []
    worker_count = []
    serialno = 0
    adults = 0
    workers = 0
    for i, row in p_PUMS.iterrows():
        if p_PUMS['SERIALNO'][i] != serialno:
            adult_count.append(adults)
            worker_count.append(workers)
            adults = 0
            workers = 0
            serialno = p_PUMS['SERIALNO'][i]
        if p_PUMS['AGEP'][i] >= 18:
            adults += 1
        if p_PUMS['ESR'][i] in [1, 2, 4, 5]:
            workers += 1
    adult_count.append(adults)
    worker_count.append(workers)
    hh_PUMS['NUM_ADULTS'] = adult_count
    hh_PUMS['NUM_WORKERS'] = worker_count

    # Saves the hh_PUMS and p_PUMS dataframes to .csv files
    hh_PUMS_save_name = ('data/seed_households_' + state + '_' + county +
                         '_' + str(year) + '.csv')
    p_PUMS_save_name = ('data/seed_persons_' + state + '_' + county +
                        '_' + str(year) + '.csv')
    hh_PUMS.to_csv(hh_PUMS_save_name, index=False)
    p_PUMS.to_csv(p_PUMS_save_name, index=False)
    logging.info('Seed households saved to: ' + hh_PUMS_save_name)
    logging.info('Seed persons saved to: ' + p_PUMS_save_name)
    print('')


# pumas = query_acs('06', '001', 2016)
# hh_vars = ['WGTP', 'NP', 'HINCP', 'NOC', 'ADJINC', 'TEN', 'SERIALNO', 'PUMA']
# p_vars = ['PWGTP', 'AGEP', 'RAC1P', 'SEX', 'RELP', 'ESR', 'SERIALNO', 'ADJINC',
#           'PINCP', 'PUMA']
# query_pums_API('06', '001', 2016, pumas, hh_vars, p_vars)
