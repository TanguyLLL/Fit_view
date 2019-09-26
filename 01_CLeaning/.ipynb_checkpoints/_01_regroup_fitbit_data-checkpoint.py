# CODE used to regroup fitbit files per topic and concact them in one df.

# Load required packages

import json
import pandas as pd
import os
import sys
import matplotlib.pyplot as plt
import ast
import numpy as np
import re
sns.set(style="darkgrid")


def regroup(fitbit_path):

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
            print(file)


