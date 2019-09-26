# CODE used to regroup fitbit files per topic and concact them in one df.

# Load required packages

import json
import pandas as pd
import os
import sys
import ast
import numpy as np
import re
import matplotlib.pyplot as plt
import seaborn as sns


# Tools and functions used in the code

def regroup_files(list_files, fitbit_path, type):
    counter = 1
    df = pd.DataFrame()
    for item in list_files:
        file = pd.read_json(fitbit_path + item)
        if type == 'heart_rate':

            #file['value'] = file['value'].apply(lambda x : ast.literal_eval(x))
            file_value = file['value'].apply(pd.Series)
            file_date = file['dateTime']
            file = file_value.join(file_date)
            

        df = pd.concat([df, file], axis=0, sort=False, ignore_index=True)
        if counter % 25 == 0:
            print(counter, " Files processed - lines remaining to process", len(list_files)- counter)
        counter += 1
    return df







def regroup(fitbit_path, data_path):

    # Listing different available table
    list_of_files = os.listdir(fitbit_path)



    time_in_heart_rate_zones_files = []
    resting_heart_rate_files = []
    heart_rate_files = []
    very_active_minutes_files = []
    lightly_active_minutes_files = []
    moderately_active_minutes_files = []
    sedentary_minutes_files = []
    steps_files = []
    sleep_files = []
    distance_files = []
    calories_files = []
    swim_lengths_data_files = []
    altitude_files = []
    exercise_files = []
    demographic_files = []

    for file in list_of_files:
        if re.search('time_in_heart_rate_zones', file):
            time_in_heart_rate_zones_files.append(file)
        elif re.search('resting_heart_rate', file):
            resting_heart_rate_files.append(file)
        elif re.search('heart_rate', file):
            heart_rate_files.append(file)
        elif re.search('very_active_minutes', file):
            very_active_minutes_files.append(file)
        elif re.search('lightly_active_minutes', file):
            lightly_active_minutes_files.append(file)
        elif re.search('moderately_active_minutes', file):
            moderately_active_minutes_files.append(file)
        elif re.search('sedentary_minutes', file):
            sedentary_minutes_files.append(file)
        elif re.search('steps', file):
            steps_files.append(file)
        elif re.search('sleep', file):
            sleep_files.append(file)
        elif re.search('distance', file):
            distance_files.append(file)
        elif re.search('calories', file):
            calories_files.append(file)
        elif re.search('swim_lengths_data', file):
            swim_lengths_data_files.append(file)
        elif re.search('altitude', file):
            altitude_files.append(file)
        elif re.search('exercise', file):
            exercise_files.append(file)
        elif re.search('demographic', file):
            demographic_files.append(file)


        # Prints out the files that will not be used
        else:
            print('Excluded file    ',file)
    print(20*'-','Builded categories and files per category ', 20*'-')
    print('time_in_heart_rate_zones_files  ', len(time_in_heart_rate_zones_files), '\n',
          'resting_heart_rate_files  ', len(resting_heart_rate_files), '\n',
          'heart_rate_files  ', len(heart_rate_files), '\n',
          'very_active_minutes_files  ', len(very_active_minutes_files), '\n',
          'lightly_active_minutes_files  ', len(lightly_active_minutes_files), '\n',
          'moderately_active_minutes_files  ', len(moderately_active_minutes_files), '\n',
          'sedentary_minutes_files  ', len(sedentary_minutes_files), '\n',
          'steps_files  ', len(steps_files), '\n',
          'sleep_files  ', len(sleep_files), '\n',
          'distance_files  ', len(distance_files), '\n',
          'calories_files  ', len(calories_files), '\n',
          'swim_lengths_data_files  ', len(swim_lengths_data_files), '\n',
          'altitude_files  ', len(altitude_files), '\n',
          'exercise_files  ', len(exercise_files), '\n',
          'demographic_files  ', len(demographic_files), '\n')


    # Resting heart regroup

    print(20*'-',"Regroup resting hearbeat data into one dataframe ", 20*'-')
    print(10*'/', 'CHECK: amount of available rows = {}'.format(len(resting_heart_rate_files)), 10*'/')

    # Regroup resting heart rate in a single dataframe

    resting_heart_rate = regroup_files(list_files=resting_heart_rate_files, fitbit_path=fitbit_path, type='rest_H')
    # Sort by date
    resting_heart_rate.sort_values(by=['dateTime'], axis=0, inplace=True)
    # Return values
    resting_heart_rate = resting_heart_rate['value'].apply(pd.Series)
    # remove null values
    resting_heart_rate = resting_heart_rate[resting_heart_rate['date'].notnull()].copy()
    # Change to date format
    resting_heart_rate['date'] = pd.to_datetime(resting_heart_rate['date'])
    print(10*'/', 'resting_heart_rate processed - size = {0}'.format(resting_heart_rate.shape),10*'/')
    print('\n', '\n')
    # Save data
    try:
        resting_heart_rate.to_csv(data_path + 'resting_heart_rate.csv')
        print('resting_heart_rate - SAVED')
    except:
        print('Could not save data - Please check path provided in data_path')


    # Plot data
    fig = plt.figure(figsize=(15, 6))
    plt.title('Resting heartbeat over time', fontsize=20)
    ax = sns.lineplot(x=resting_heart_rate['date'], y=resting_heart_rate['value'])
    plt.xticks(rotation=70)
    plt.show()

    print('\n', '\n')
    print(20*'-',"Regroup hearbeat data into one dataframe ", 20*'-')
    print('If several months of data are provided, processing can take some time')
    print(10*'/',"Amount of files available = {0}".format(len(heart_rate_files)),10*'/')
    ####################################################################################

    # Regroup heart rate in one df and save
    heart_rate =  regroup_files(heart_rate_files,fitbit_path, type='heart_rate')
    print(10*'/', 'Heart_rate processed - size = {0}'.format(heart_rate.shape),10*'/')
    try:
        heart_rate.to_csv(data_path + 'heart_rate_uncorrected.csv', sep=';')
        print('heart_rate SAVED')
    except:
        print('Could not save data - Please check path provided in data_path')



    return 'tttt'


