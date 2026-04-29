#########################################################################
####### The function model_pipeline take a dataframe as an argument #####
####### It could be ammended to take a CSV as well ######################
#########################################################################

import numpy as np
import pandas as pd


def model_pipeline(df):

    cols = df.iloc[:,4:].select_dtypes(exclude = ['object']).columns
    df[cols] = df[cols].apply(pd.to_numeric, downcast='float', errors='coerce')

    # Define the weights for each column
    weights = {
    'airtime_in_90days': 0.02,
    'bill_payment_in_90days': 0.02,
    'cable_tv_in_90days': 0.02,
    'deposit_in_90days': 0.3,
    'easy_payment_in_90days': 0.02,
    'farmer_in_90days': 0.02,
    'inter_bank_in_90days': 0.02,
    'mobile_in_90days': 0.02,
    'withdrawal_in_90days': 0.5
    }

    # Calculate the weighted sum
    df['estimates'] = df[list(weights.keys())].mul(list(weights.values())).sum(axis=1)
    
    def model_output(default_in_last_90days:str, has_it_make_it_good:str, estimates):
        if default_in_last_90days == 'N':
            return round(25 + 1.0*estimates, -3) # round to the nearest 1000
        
        elif default_in_last_90days == 'Y' and has_it_make_it_good == 'Y':
            return round(25 + 0.5*estimates, -3)
        
        elif default_in_last_90days == 'Y' and has_it_make_it_good == 'N':
            return 0
        
    df['amount_approved'] = df.apply(lambda x: model_output(x['default_in_last_90days']
                                                            ,x['has_it_make_it_good']
                                                            ,x['estimates']), axis=1)
    
    del df['estimates']
    
    def decline_flag(df):
        if (df['amount_approved'] == 0 and df['default_in_last_90days'] == 'Y' and df['has_it_make_it_good'] == 'N'):
            return 'Loan declined due to defaults'
        elif (df['amount_approved'] == 0):
            return 'Loan declined due to low transaction or incomplete records'
        else:
            return ' '
        
    df['decline_reason'] = df.apply(decline_flag, axis=1)
    
    return df
