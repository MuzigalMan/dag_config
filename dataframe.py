from scraper import scraper as sc
import pandas as pd
from reparser import parser
from config import conn
import os


def getData(job,location):
    scraper = sc(job,location)
    data = scraper.linkedin_data_1()

def main(data:list): 
    #get data
    # get_job = list(data.items())[0]
    name = data[0]
    job = data[1]
    # get_location = list(data.items())[1]
    location = data[2]

    #resume parser
    user = parser(name)
    userExp = user['exp']
    userSkill = user['skills']

    if userExp in (1,2,3):
        userLevel = ["Entry level","Internship","Associate"]
    elif userExp >= 4 and userExp <= 10:
        userLevel = ["Mid-Senior level","Executive"]
    elif userExp > 10:
        userLevel = "Director"
        
    # scraper
    getData(job,location)

    data = pd.read_csv(os.getenv('CSVDATA'))
    data.drop(data.filter(regex="Unname"),axis=1, inplace=True)
    data['Date'] = pd.to_datetime(data['Date'])

    if isinstance(userLevel, str):
        df_filtered = data[data['Experience'] == userLevel]
    elif isinstance(userLevel, list):
        df_filtered = data[data['Experience'].isin(userLevel)]
    else:
        raise ValueError('userLevel must be a string or a list')
    

    df_filtered['Matched Skills'] = df_filtered['JD'].apply(lambda x: [s for s in userSkill if s.lower() in x.lower()])

    df_filtered = df_filtered[df_filtered['Matched Skills'].apply(len) >= 5]

    df_filtered = df_filtered.sort_values('Date', ascending=False)

    df_filtered['Matched Skills'] = df_filtered['Matched Skills'].apply(lambda x: ', '.join(x))

    df_filtered['job_code'] = job
    
    df_filtered['user_name'] = name.split('_')[0]
    
    dag_table = pd.read_sql_query("SELECT * FROM dag_config",conn)
    
    def find_missing_rows(df1, df2):
        merged_df = pd.merge(df1, df2, indicator=True, how='left')
        missing_rows = merged_df.loc[merged_df['_merge'] == 'left_only']
        missing_rows = missing_rows.drop(columns='_merge')
        return missing_rows
    
    final_df = find_missing_rows(df_filtered,dag_table)
    
    conn.execute('SET SQL_REQUIRE_PRIMARY_KEY = OFF;')
    final_df.to_sql('dag_config', conn, if_exists='append', index=False)
