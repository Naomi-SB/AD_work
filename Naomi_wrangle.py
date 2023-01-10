

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
    df.start_date = pd.to_datetime(df.start_date)
    df.end_date = pd.to_datetime(df.end_date)
    df.created_at = pd.to_datetime(df.created_at)
    df.updated_at = pd.to_datetime(df.updated_at)
    df.date = pd.to_datetime(df.date)
    
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
    df.drop(['date', 'deleted_at', 'program_id', 'path'], axis = 1, inplace = True)
    
    return df
    
def find_anomalies(df, user, span, weight, plot=False):
    '''
    THIS IS NOT NAOMI'S
    This function returns the records where a user's daily activity exceeded the upper limit of a bollinger band range
    '''
    
    # Reduce dataframe to represent a single user
    pages_one_user = one_user_df_prep(df, user)
    
    # Add bollinger band data to dataframe
    my_df = compute_pct_b(pages_one_user, span, weight, user)
    
    # Plot data if requested (plot=True)
    if plot:
        plot_bands(my_df, user)
    
    # Return only records that sit outside of bollinger band upper limit
    return my_df[my_df.pct_b>1]

def plot_bands(my_df, user):
    '''
    THIS IS NOT NAOMI'S
    This function plots the bolliger bands of the page views for a single user
    '''
    fig, ax = plt.subplots(figsize=(12,8))
    ax.plot(my_df.index, my_df.pages_one_user, label='Number of Pages, User: '+str(user))
    ax.plot(my_df.index, my_df.midband, label = 'EMA/midband')
    ax.plot(my_df.index, my_df.ub, label = 'Upper Band')
    ax.plot(my_df.index, my_df.lb, label = 'Lower Band')
    ax.legend(loc='best')
    ax.set_ylabel('Number of Pages')
    plt.show()

################################################################### DATA FRAMES FOR VISUALIZATION    












