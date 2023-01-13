

import warnings
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
import env



def wrangle_curriculum_data():
    '''
    This function acquires data from SQL database
    with inplace env.py file, changes columns 
    with date information to datetime objects,
    sets the index to 'date', renames and 
    replaces values for readability, and drops
    values that aren't used or are null.
    '''
    url = f'mysql+pymysql://{env.user}:{env.password}@{env.host}/curriculum_logs'
    query = '''
    SELECT *
    FROM logs
    LEFT JOIN cohorts ON cohorts.id= logs.cohort_id
   '''
    # pulls in data as a df
    df = pd.read_sql(query, url)

    #converts columns with date infor to dt objects
    
    df.date = pd.to_datetime(df.date)
    df.start_date = pd.to_datetime(df.start_date)
    df.end_date = pd.to_datetime(df.end_date)
    df.created_at = pd.to_datetime(df.created_at)
    df.updated_at = pd.to_datetime(df.updated_at)
    # sets index as dt object
    df = df.set_index(df.date)
    
    # creates column caled pages that is the number of paths per day
    pages = df['path'].resample('d').count()
    
    # name changes
    df['program_name']= df.program_id
    df['end_page'] = df.path
    
    # making programs human-readable
    df.program_name = df.program_name.replace({1.0:'full_stack_php',2.0:'full_stack_java',3.0:'data_science',4.0:'front_end'})
    
    # elimating staff from df
    df = df[df.name != 'Staff']
    
    # dropping nulls and unnecessary columns
    df.fillna('none', inplace = True)
    df.drop(['deleted_at', 'program_id', 'path'], axis = 1, inplace = True)
    
    return df
    





