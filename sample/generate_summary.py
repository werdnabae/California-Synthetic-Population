import pandas as pd
import numpy as np
import os

file_list = os.listdir('all_outputs/')

final_summary_files = [file for file in file_list if
                       file.startswith('final_summary')]

dataframes = []
for file in final_summary_files:
    df = pd.read_csv('all_outputs/' + file)
    dataframes.append(df)
combined_df = pd.concat(dataframes, ignore_index=True)


def do_summary(combined_df):
    categories = ['num_hh',
                  'children_yes', 'children_no',
                  'income0', 'income1', 'income2', 'income3', 'income4',
                  'income5',
                  'income6', 'income7',
                  'HHSIZE1', 'HHSIZE2', 'HHSIZE3', 'HHSIZE4', 'HHSIZE5',
                  'HHSIZE6',
                  'HHSIZE7',
                  'hh_own', 'hh_rent',
                  'work0', 'work1', 'work2', 'work3',
                  'num_persons', 'age_lt19', 'age_20-35', 'age_35-60',
                  'age_60+',
                  'race_white', 'race_black', 'race_asian', 'race_other',
                  'sex_male', 'sex_female']

    columns_to_exclude = ['geography', 'id']
    sum_values = combined_df.drop(columns=columns_to_exclude).sum()
    sum_df = pd.DataFrame([sum_values], columns=sum_values.index)

    diff_df = pd.DataFrame()
    diff_df_sum = pd.DataFrame()

    for cat in categories:
        diff_name = cat + '_diff'
        control_name = cat + '_control'
        diff_df[diff_name] = abs(combined_df[diff_name]) / combined_df[
            control_name] * 100
        diff_df_sum[diff_name] = sum_df[diff_name] / sum_df[control_name] * 100

    diff_cats = [s + '_diff' for s in categories]
    stats = diff_df[diff_cats].replace([np.inf, -np.inf], np.nan).dropna().describe()
    # stats.to_csv('alameda_stats_popsim.csv')

    MSE_df = {}
    MAB = {}
    aggregate_diff = {}
    for cat in categories:
        diff_name = cat + '_diff'
        mse = np.mean(combined_df[diff_name] ** 2)
        MSE_df[diff_name] = mse
        MAB[diff_name] = np.mean(np.abs(combined_df[diff_name]))
        aggregate_diff[diff_name] = sum_df[diff_name][0]

    MSE = pd.DataFrame(MSE_df, index=['MSE'])
    MAB = pd.DataFrame(MAB, index=['MAB'])
    aggregate_diff = pd.DataFrame(aggregate_diff, index=['aggregate # diff'])

    combined = pd.concat([stats, MSE, MAB, aggregate_diff, diff_df_sum], axis=0)
    combined = combined.rename(index={0: 'aggregate % diff'})
    combined = combined.T
    # combined.to_csv('alameda_stats_popsim.csv')

    combined = combined.rename(columns={'mean': 'MAPE', '50%': 'MdAPE'})
    combined = combined[
        ['MAPE', 'MdAPE', 'aggregate # diff', 'aggregate % diff']]
    return combined

# all_counties = do_summary(combined_df)
# writer = pd.ExcelWriter('all_outputs/synthesis_stats.xlsx', engine='openpyxl')
# all_counties.to_excel(writer, sheet_name='sheet1')
# writer.save()
all_counties = do_summary(combined_df)
output_file_path = 'all_outputs/synthesis_stats.xlsx'

# Create an ExcelWriter object with the 'openpyxl' engine
with pd.ExcelWriter(output_file_path, engine='openpyxl') as writer:
    # Write the DataFrame to the Excel file with the specified sheet name
    all_counties.to_excel(writer, sheet_name='all counties')

    for file in final_summary_files:
        df = pd.read_csv('all_outputs/' + file)
        county = do_summary(df)
        sheet_name = file[len('final_summary_TRACT_'):-4]

        # Write the county DataFrame to a new sheet in the existing Excel file
        county.to_excel(writer, sheet_name=sheet_name)
# all_counties.to_excel('all_outputs/synthesis_stats.xlsx')
