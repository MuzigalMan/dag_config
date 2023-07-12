from scraper import scraper as sc
import pandas as pd
from reparser import parser
from config import conn


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

    data = pd.read_csv('./data/job_data')
    data['Date'] = pd.to_datetime(data['Date'])

    if isinstance(userLevel, str):
        df_filtered = data[data['Experience'] == userLevel]
    elif isinstance(userLevel, list):
        df_filtered = data[data['Experience'].isin(userLevel)]
    else:
        raise ValueError('userLevel must be a string or a list')
    
    # scraper
    getData(job,location)

    df_filtered['Matched Skills'] = df_filtered['JD'].apply(lambda x: [s for s in userSkill if s.lower() in x.lower()])

    df_filtered = df_filtered[df_filtered['Matched Skills'].apply(len) >= 5]

    df_filtered = df_filtered.sort_values('Date', ascending=False)

    df_filtered['Matched Skills'] = df_filtered['Matched Skills'].apply(lambda x: ', '.join(x))

    df_filtered['job_code'] = job
    
    df_filtered['user_name'] = name.split('_')[1]
    
    # dag_table = pd.read_sql_query("SELECT * FROM dag_config")
    
    # def find_missing_rows(df1, df2):
    #     merged_df = pd.merge(df1, df2, indicator=True, how='left')
    #     missing_rows = merged_df.loc[merged_df['_merge'] == 'left_only']
    #     missing_rows = missing_rows.drop(columns='_merge')
    #     return missing_rows
    
    # final_df = find_missing_rows(df_filtered,dag_table)
    
    conn.execute('SET SQL_REQUIRE_PRIMARY_KEY = OFF;')
    df_filtered.to_sql('dag_config', conn, if_exists='replace', index=False)


# getData("Full Stack Developer","Hyderabad") 
# temp = {'job': 'watchman', 'location':'hyd'}
# main(temp)