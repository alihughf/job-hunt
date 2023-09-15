import json
import pandas as pd
import csv
import glob

def read_accounts_data(accounts_file_paths):
    accounts_df = pd.DataFrame()

    for file_path in accounts_file_paths:
        with open(file_path, 'r') as json_file:
            data = json.load(json_file)
            #filtering keys not used in exercise
            data['account'] = data['account'].get('user',None)
            df = pd.json_normalize(data)
            accounts_df = pd.concat([accounts_df,df], axis=0)
    accounts_df.reset_index(drop=True,inplace=True)
    return accounts_df

def read_reports_data(file_paths):
    reports_df = pd.DataFrame()
    for file_path in file_paths:
        with open(file_path, 'r') as json_file:
            data = json.load(json_file)
            #filtering keys not used in exercise
            remove_lst = ["Accounts",
                        "Addresses",
                        "Defaults", "Detect",
                        "Employers",
                        "Enquiry",
                        "InputData",
                        "Judgements",
                        "ReturnData",
                        "Session",
                        "SubscriberInfo",
                        "Telephones"]
            [data['report'].pop(key, None) for key in remove_lst]
            if len(data['report']['ScoreBlock']['Delphi']) ==1:
                data['Delphi'] = data['report']['ScoreBlock']['Delphi'][0]
            else:
                print("warning more than one delphi!")
            df = pd.json_normalize(data)
            reports_df = pd.concat([reports_df,df], axis=0)
    reports_df.reset_index(drop=True,inplace=True)
    reports_df['pulled-timestamp'] = pd.to_datetime(reports_df['pulled-timestamp'])
    return reports_df

def calculate_average_credit_score(reports_df):
    average_credit_score = reports_df['report.ScoreBlock.Delphi.Score'].mean()

    return average_credit_score

def count_users_by_employment_status(accounts_df):
    employment_status_counts = accounts_df['account.employmentStatus'].value_counts()
    
    return employment_status_counts

def count_users_in_score_ranges(reports_df):
    reports_df['latest_score'] = reports_df['report.ScoreBlock.Delphi'].apply(lambda x: int(x[0]['Score']))
    sorted_reports_df = reports_df.sort_values(by='latest_score')

    # Define the score bins and labels
    score_bins = range(0, max(reports_df['latest_score']), 50)
    score_labels = [f"{score_bins[i]} - {score_bins[i+1]}" for i in range(len(score_bins)-1)]
    sorted_reports_df = reports_df.sort_values(by='latest_score')
    # Use pd.cut() to categorize the scores into bins
    sorted_reports_df['score_range'] = pd.cut(sorted_reports_df['latest_score'], bins=score_bins, labels=score_labels, right=False)

    # Group by the score range and count rows in each group
    score_counts = sorted_reports_df.groupby('score_range').size().reset_index(name='count')

    # Now, score_counts is a DataFrame containing the count of rows in each score range
    return score_counts

    
def enrich_bank_data(accounts_df, reports_df):
    # Task 4: Enriched Bank Data

    accounts_df.dropna(subset=['account.id'], inplace=True)
    accounts_df['account.id'] = accounts_df['account.id'].astype(int)
    reports_df.dropna(subset=['account-id'], inplace=True)
    reports_df['account-id'] = reports_df['account-id'].astype(int)

    merged_df = pd.merge(accounts_df, reports_df, left_on='account.id', right_on='account-id')
    enriched_bank_df = merged_df[['accountId', 'account.employmentStatus', 'account.bankName','report.Summary.Payment_Profiles.CPA.Bank.Total_number_of_Bank_Active_accounts_', 'report.Summary.Payment_Profiles.CPA.Bank.Total_outstanding_balance_on_Bank_active_accounts']]
    enriched_bank_df.columns = ['user-uuid', 'employment_status', 'bank_name', 'no. active bank accounts', 'total outstanding balance']
    
    return enriched_bank_df

if __name__ == "__main__":
    accounts_file_paths = glob.glob('bulk-reports/accounts/*.json')
    reports_file_paths = glob.glob('bulk-reports/reports/*/*/*/*.json')

    # Read data from JSON files
    accounts_data = read_accounts_data(accounts_file_paths)
    reports_data = read_reports_data(reports_file_paths)
    reports_data['pulled-timestamp'] = pd.to_datetime(reports_data['pulled-timestamp'])
    recent_indices = reports_data.groupby('account-id')['pulled-timestamp'].idxmax()
    most_recent_df = reports_data.loc[recent_indices]
    most_recent_df['account-id'].value_counts()

    # Perform tasks
    average_credit_score = calculate_average_credit_score(reports_data)
    employment_status_counts = count_users_by_employment_status(accounts_data)
    score_range_counts = count_users_in_score_ranges(most_recent_df)
    enriched_bank_df = enrich_bank_data(accounts_data, most_recent_df)

    # Output results to CSV files
    average_credit_score.to_csv('average_credit_score.csv', index=False, header=['Average Credit Score'])
    employment_status_counts.reset_index().to_csv('employment_status_counts.csv', index=False, header=['Employment Status', 'Count'])
    score_range_counts.reset_index().to_csv('score_range_counts.csv', index=False, header=['Score Range', 'Count'])
    enriched_bank_df.to_csv('enriched_bank_data.csv', index=False)
