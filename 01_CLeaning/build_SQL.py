
import json
import pandas as pd
import os
import sys
import numpy as np
import re
import sqlite3
from functools import reduce


# Usefull paths : to adapt with local path where the data is stored
# This is the only data that has to be changed by the user


pd.options.display.max_rows = 999
pd.options.display.max_columns=200
pd.set_option('max_columns', 500)
pd.set_option('max_rows', 20000000)
pd.set_option('max_colwidth', 5000)


# Return a tables based on a query
def run_query(data_path, q):
    with sqlite3.connect(data_path+'fitbit.db') as conn:
        return pd.read_sql(q, conn)

# Function that runs queries but dont return tables
def run_command(c):
    with sqlite3.connect(db) as conn:
        conn.isolation_level = None
        conn.execute(c)

def show_tables(data_path):
    q = '''SELECT
        name
    FROM sqlite_master
    WHERE type IN ("table");'''
    df =run_query(data_path, q)
    tb_list = list(df['name'])
    return tb_list




def table_exist(con):
    cursorObj = con.cursor()

    cursorObj.execute('SELECT name from sqlite_master where type= "table"')

    x = cursorObj.fetchall()

    print(list(x))
    return x










#---------------------------------------------------------------------------------------

def list_tables(fitbit_path):
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
            print('Excluded file    ', file)

    print(20 * '-', 'Builded categories and files per category ', 20 * '-')
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


    list_of_lists={
                      'time_in_heart_rate_zones':time_in_heart_rate_zones_files,
                      'resting_heart_rate':resting_heart_rate_files,
                      'heart_rate':heart_rate_files,
                      'very_active_minutes':very_active_minutes_files,
                      'lightly_active_minutes':lightly_active_minutes_files,
                      'moderately_active_minutes':moderately_active_minutes_files,
                      'sedentary_minutes':sedentary_minutes_files,
                      'steps':steps_files,
                      'sleep':sleep_files,
                      'distance':distance_files,
                      'calories':calories_files,
                      'swim_lengths_data':swim_lengths_data_files,
                      'altitude':altitude_files,
                      'exercise':exercise_files,'demographic':demographic_files}
    return list_of_lists

# -----------------------------------------------------------------------------------------------





def to_sql(data_path, fitbit_path):


    list_dico = list_tables(fitbit_path)
    conn= sqlite3.connect(data_path+ 'fitbit.db')




    ##### CODE USED TO BUILD resting heart rate TABLE #####

    active_minutes_list=['sedentary_minutes','lightly_active_minutes', 'moderately_active_minutes',
                         'very_active_minutes']

    t_list = show_tables(data_path)
    df_dico={}
    temp_list= []
    if 'minutes_in_zones' not in t_list:

        conn.execute('''
        CREATE TABLE IF NOT EXISTS "minutes_in_zones" (
        "dateTime" TIMESTAMP NOT NULL ,
        "value_sedentary_minutes" INTEGER,
        "value_lightly_active_minutes" INTEGER,
        "value_moderately_active_minutes" INTEGER,
        "value_very_active_minutes" INTEGER,
        PRIMARY KEY (dateTime));
        ''').fetchall()

        for name in active_minutes_list:
            df_dico[name]= pd.DataFrame()
            for file in list_dico[name]:
                df= pd.read_json(fitbit_path+file)
                df_dico[name] = pd.concat([df, df_dico[name]])
            df_dico[name]= df_dico[name].rename(columns= {'value':'value'+'_'+name})
            temp_list.append(df_dico[name])
        df_final= reduce(lambda left,right: pd.merge(left,right, on='dateTime'), temp_list)
        df_final.to_sql('minutes_in_zones',conn, index=False, if_exists='append')
    else:
        print('Table minutes_in_zones already exists')


    #----------------------------------------------------------------------------

    ##### CODE USED TO BUILD time_in_heart_rate_zones TABLE #####

    t_list = show_tables(data_path)

    if 'time_in_heart_rate_zones' not in t_list:

        conn.execute('''
        CREATE TABLE IF NOT EXISTS "time_in_heart_rate_zones" (
        "dateTime" TIMESTAMP NOT NULL ,
        "valuesInZones.BELOW_DEFAULT_ZONE_1" REAL,
        "valuesInZones.IN_DEFAULT_ZONE_1" REAL,
        "valuesInZones.IN_DEFAULT_ZONE_2" REAL,
        "valuesInZones.IN_DEFAULT_ZONE_3" REAL,
        PRIMARY KEY (dateTime));
        ''').fetchall()


        for file in list_dico['time_in_heart_rate_zones']:
            phase_1 = pd.read_json(fitbit_path + file)
            phase_2 = phase_1['value']
            phase_3 = pd.io.json.json_normalize(phase_2)
            phase_4 = phase_1.merge(phase_3, left_on= phase_1.index, right_on=phase_3.index, how='outer')
            phase_4.drop(labels=['value', 'key_0'], axis=1, inplace=True)
            phase_4.to_sql('time_in_heart_rate_zones', conn, if_exists='append', index=False)
    else:
        print('Table "time_in_heart_rate_zone" already exists')


    #----------------------------------------------------------------------------

    ##### CODE USED TO BUILD heart_rate TABLE #####
    # - !!!! THIS TABLE CAN TAKE TIME TO BE BUILD and reach a size of several million rows

    t_list = show_tables(data_path)
    counter = 0


    if 'heart_rate' not in t_list:

        conn.execute('''
        CREATE TABLE IF NOT EXISTS "heart_rate" (
        "bpm" REAL ,
        "confidence" REAL,
        "dateTime" TIMESTAMP NOT NULL,
        PRIMARY KEY (dateTime));
        ''').fetchall()

        for file in list_dico['heart_rate']:
            data = pd.read_json(fitbit_path + file)
            value = pd.io.json.json_normalize(data['value'])
            final = value.merge(data['dateTime'], left_on=data.index, right_on=value.index, how='outer')
            final.drop(columns='key_0', inplace=True)
            final.to_sql('heart_rate',conn, if_exists='append', index=False)
            counter += 1
            if counter % 40 == 0:
                print('File transformed = {0} - {1} % done!  '.format(counter,np.around(
                                                                      counter / len(list_dico['heart_rate']) * 100,decimals= 3)))
    else:
        print('Table "heart_rate" already exists')


    #----------------------------------------------------------------------------
    ##### CODE USED TO BUILD resting heart rate TABLE #####

    t_list = show_tables(data_path)

    if 'resting_heart_rate' not in t_list:

        conn.execute('''
        CREATE TABLE IF NOT EXISTS "resting_heart_rate" (
        "date" TIMESTAMP NOT NULL ,
        "error" FLOAT,
        "value" FLOAT,
        PRIMARY KEY (date));
        ''').fetchall()



        for file in list_dico['resting_heart_rate']:
            data = pd.read_json(fitbit_path + file)
            value = pd.io.json.json_normalize(data['value'])

            value = value[value['date'].notnull()]  # remove empty lines
            value['date'] = pd.to_datetime(value['date'])
            value = value.loc[value['error'] < 30, :] # remove rows with super high error rate
            value.to_sql('resting_heart_rate', conn, if_exists='append', index=False)
    else:
        print('Table resting_heart_rate already exists')



    #----------------------------------------------------------------------------





    return t_list











#conn.close()
#conn= sqlite3.connect(data_path+ 'fitbit.db')


