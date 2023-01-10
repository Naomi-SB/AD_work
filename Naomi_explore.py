import warnings
warnings.filterwarnings("ignore")
import matplotlib.pyplot as plt

import os
import numpy as np
import pandas as pd
import seaborn as sns
import env

import Naomi_wrangle as w

def get_cohort_top10_paths(cohort, name_path):
    
    ''' This function prints the 10 most accessed paths for the given cohort.'''
    df = name_path[name_path.name == cohort]
    df = df[df.path != 'toc']
    df = df[df.path != '/']
    df = df[df.path != 'search/search_index.json']
    print(f'{cohort} top 10 lessons accessed:\n')
    return print(df.path.value_counts().head(10))

def get_cohort_bottom10_paths(cohort, name_path):
    
    ''' This function prints the 10 least accessed paths for the given cohort.'''
    df = name_path[name_path.name == cohort]
    df = df[df.path != 'toc']
    df = df[df.path != '/']
    df = df[df.path != 'search/search_index.json']
    print(f'{cohort} bottom 10 lessons accessed:\n')
    return print(df.path.value_counts().tail(10))

def seperate_students_webdev_vs_datasci(df): 
    '''
    seperate_students_webdev_vs_datasci takes in a dataframe and seperates the webdev students from the data science student
    returns two data frames, webbdev_students and datascri_students
    '''
    #create column for data webdev students vs data science students
    df['web_yes']= df['program_id']
    df.web_yes = df.web_yes.replace({1.0: 1,2.0:1,3.0:0,4.0:1})
    # webdev students only
    webdev_students= df[df.web_yes==1]
    # data science students only
    datasci_students = df[df.web_yes==0]
    
    return webdev_students, datasci_students
def get_double_acess(df,keywords):
    '''
    get_double_acess takes in a dataframe and a list of the top keywords in a program to reference the string path
    for the keyword and returns a dataframe of students curriculum logs that have acess the program.
    '''
    data_keywords = keywords
    double_cross=pd.DataFrame()
    for i in data_keywords:
        temp= df[df.path.str.contains(i)]
        double_cross = pd.concat([double_cross, temp], axis=0,ignore_index=True)
    return double_cross