################################ Background functions ################################
def readCSV(csvFileName):
    import csv, sys
    csvdataRows = []
    with open(csvFileName) as csvfile:
        spamreader = csv.reader(csvfile)
        #for line in data:
        for row in spamreader:
            csvdataRows.append(row)
    ## Return rows #
    return csvdataRows

def writeToCSV(csvFileName, csvdataRows):
    import csv, sys
    if sys.version_info >= (3,0,0):
        csvfile = open(csvFileName, 'w', newline='')
    else:
        csvfile = open(csvFileName, 'wb')
    spamwriter = csv.writer(csvfile)
    for row in csvdataRows:
        try:
            spamwriter.writerow(row)
        except:
            print("Failed at " + str(row))
    return
################################ End Background functions ################################

################################ [1] Build training time-series from raw data logs ################################

csvFileName = 'Book2.csv'
csvdataRows = readCSV(csvFileName)

max([len(row) for row in csvdataRows])#1
len([row for row in csvdataRows[1:] if 'w|rmc\\' not in row[0]])#562 --> about half...
# date
rows_with_usernames = [row[0] for row in csvdataRows[1:] if 'w|rmc\\' in row[0]]
usernames = [row[row.index('w|rmc\\')+len('w|rmc\\'):row.index(' ', row.index('w|rmc\\')+len('w|rmc\\'))] for row in rows_with_usernames]
len(set(usernames))
[el for el in rows_with_usernames if '.mp4' in el]
[el[0] for el in csvdataRows[1:] if '.mp4' in el[0] and 'w|rmc\\' not in el[0]]#6

len([row[0] for row in csvdataRows[1:] if '/Departments/Training/RegionalTube/' in row[0]])# how many diff things after?

def readLOG(logFileName):
    file = open(logFileName, 'r')
    logdataRows = file.read().splitlines()
    file.close()
    return logdataRows

def build_aggregate_log_csv():
    import os
    log_files = ['Log Files/'+i for i in os.listdir('Log Files') if ('.log' in i)]
    csvdataRows = []
    for logFileName in log_files:
        print(str(log_files.index(logFileName)) + ' of ' + str(len(log_files)))
        # [1] Transform to csv
        logdataRows = readLOG(logFileName)
        # [2] Add to csvdataRows
        csvdataRows += logdataRows#28,369,054
    # Save?
    return csvdataRows

def build_relevant_rows(csvdataRows):
    # Get rid of row [a] rows without usernames and [b] rows that don't have .mp4 and [c] and rows that don't include /Departments/Training/RegionalTube/ (e.g., /Departments/Training/Webinars/)
    relevant_rows = [row for row in csvdataRows if ('w|rmc\\' in row) and ('.mp4' in row) and ('/Departments/Training/RegionalTube/' in row)]#9,874 --> 7,953
    return relevant_rows

def build_data_frame(relevant_rows):
    from datetime import datetime
    import pandas as pd
    structured_data = [{'datetime': datetime.strptime(row[:len('2021-05-03 16:39:13')], '%Y-%m-%d %H:%M:%S'), 'username':row[row.index('w|rmc\\')+len('w|rmc\\'):row.index(' ', row.index('w|rmc\\')+len('w|rmc\\'))], 'url':row[row.index('/'):row.index(' ', row.index('/'))]} for row in relevant_rows]
    df = pd.DataFrame(structured_data)
    return df

def build_time_series(df):
    import pandas as pd
    from statistics import mean, stdev
    df['username'].unique()#1101 --> 1004
    df['url'].unique()#231 --> 122
    username_count_dict = {username: len(df[df['username'].isin([username])]) for username in df['username'].unique()}
    high_activity_users = [username for username in list(username_count_dict.keys()) if username_count_dict[username]>(mean(list(username_count_dict.values()))+2*stdev(list(username_count_dict.values())))]
    ts = df.set_index('datetime')
    monthly = ts[ts['username'].isin(['mwettschurack'])].groupby(pd.Grouper(freq='M'))
    monthly.count()
    for username in high_activity_users:
        print(username)
        monthly = ts[ts['username'].isin([username])].groupby(pd.Grouper(freq='M'))
        monthly.count()
        print(' ')
    return time_series

# Run
csvdataRows = build_aggregate_log_csv()
relevant_rows = build_relevant_rows(csvdataRows)
df_training = build_data_frame(relevant_rows)
ts_training = df_training.set_index('datetime')
df_training_dictionary = dict(ts_training.groupby(['username',pd.Grouper(freq='M')]).count())
df_training_dictionary_strings = {(key[0],key[1].to_pydatetime().strftime("%Y-%m-%d")):df_training_dictionary['url'][key] for key in list(df_training_dictionary['url'].keys())}

################################ End [1] Build training time-series from raw data logs ################################

# Questions
## What are those with .mp4 in title + no rmc [username]? Can we tie these back to users (previous action similar url structure?)
## Two formats for people watching videos?

# Goals
## [a] Parse IT logs --> training usage
#### Appropriate format/structure to relevant data (Training/Webinars vs. Training/RegionalTube)
#### Build time series
## [b] Map employee usertags to full names
## [c] Map employees to start date (and new hires in {0,1})
## [d] Map employees to branch
## [e] Map training usage to sales
## [f] Map other acticities on IT intranetlogs? (e.g., actually making a loan etc?)

## sales data by month (typically how targets are set up)

################################ [2] Map training time-series to name, start dates (new hires in {0,1}), branch ################################
# [2.a] Read in "Active Team Member List"
# [2.b] For those with emails, grab user_id
# [2.c] Grab name
# [2.d] Grab title/job/role
# [2.e] Grab branch
# [2.f] % match from time-series to "Active Team Member List"

# [2.a] Read in "Active Team Member List"
csvFileName = 'regional-management-data/Active Team Member List.csv'
csvdataRows = readCSV(csvFileName)
# [2.b] For those with emails, grab user_id
relevant_csvdataRows = [csvdataRows[0]] + [row for row in csvdataRows[1:] if (row[csvdataRows[0].index('Email')] not in ['None','none', 'NONE',None]) and ('@region' in row[csvdataRows[0].index('Email')].lower())]#3996-->3911
#possible_email_permutations = list(set([row[csvdataRows[0].index('Email')][row[csvdataRows[0].index('Email')].index('@'):] for row in relevant_csvdataRows[1:]]))
relevant_csvdataRows_with_user_id = [relevant_csvdataRows[0]+['user_id']]#1774
for row in relevant_csvdataRows[1:]:
    email = row[csvdataRows[0].index('Email')]
    if '@region' in email:
        #row.append(email[:email.index('@region')])
        relevant_csvdataRows_with_user_id.append(row+[email[:email.index('@region')]])
# [2.c] Grab name
# [2.d] Grabe title/job/role
# [2.e] Grab branch
# [2.f] % match from time-series to "Active Team Member List"
time_series_unique_user_ids = [i.lower() for i in list(set(df['username'].tolist()))]
active_team_member_list_unique_user_ids = list(set([row[relevant_csvdataRows_with_user_id[0].index('user_id')].lower() for row in relevant_csvdataRows_with_user_id[1:]]))
len([i for i in time_series_unique_user_ids if (i in active_team_member_list_unique_user_ids)])/len(time_series_unique_user_ids)#0.43824701195219123

alternative_active_team_member_list_unique_user_ids = list(set([row[csvdataRows[0].index('\ufeffFirst Name')][0].lower()+row[csvdataRows[0].index('Last Name')].lower() for row in csvdataRows[1:]]))
len([i for i in time_series_unique_user_ids if (i in alternative_active_team_member_list_unique_user_ids)])/len(time_series_unique_user_ids)#0.7878486055776892
missing_entries = [i for i in time_series_unique_user_ids if (i not in alternative_active_team_member_list_unique_user_ids)]

# NAME-MATCHING 2.0
def name_match(user_id, csvdataRows):
    #### Finds the matched_index of a single real user_id in the Active Team Member List ####
    #### TRAINING LOG USERNAME --> ACTIVE TEAM MEMBER LIST INDEX (AND ULTIMATELY EMPLOYER NUMBER) ####
    #### Input 1: user_id is an actual user_id - e.g., 'msmith' from time_series_unique_user_ids ####
    #### Input 2: csvdataRows is from 'Active Team Member List.csv' ####
    import re
    from datetime import datetime
    manual_match_dict = {'jnhollingsworth':'5411', 'summerntaylor':'5199', 'mscoppedge':'3796'}
    # [0] Build list of all possible (seen) permutations
    first_initial_last_name_list = []
    first_initial_last_name_multiple_surnames_list = []
    first_name_last_name_list = []
    email_format_list = []
    employee_number_list = []
    termination_date_list = []
    user_status_list = []
    for row in csvdataRows:
        first_name = row[csvdataRows[0].index('\ufeffFirst Name')]
        last_name = row[csvdataRows[0].index('Last Name')]
        last_name_multiple_surnames = last_name.split()[len(last_name.split())-1] if (len(last_name.split())>1) else last_name
        email_format = row[csvdataRows[0].index('Email')][:row[csvdataRows[0].index('Email')].index('@')] if ((row[csvdataRows[0].index('Email')] not in ['None','none', 'NONE',None]) and ('@region' in row[csvdataRows[0].index('Email')].lower())) else 'None'
        employee_number = row[csvdataRows[0].index('Employee Number')]
        termination_date = 'None' if (row[csvdataRows[0].index('Termination Date')] in ['N/A', 'N//A', 'N\\A', 'Termination Date']) else datetime.strptime(row[csvdataRows[0].index('Termination Date')], '%m/%d/%y') if len(row[csvdataRows[0].index('Termination Date')][row[csvdataRows[0].index('Termination Date')].rfind('/')+len('/'):])==2 else datetime.strptime(row[csvdataRows[0].index('Termination Date')], '%m/%d/%Y')
        user_status = row[csvdataRows[0].index('User Status')]
        #
        first_initial_last_name_list.append(first_name.lower()[0]+last_name.lower())
        first_initial_last_name_multiple_surnames_list.append(first_name.lower()[0]+last_name_multiple_surnames.lower())
        first_name_last_name_list.append(first_name.lower()+last_name.lower())
        email_format_list.append(email_format.lower())
        employee_number_list.append(employee_number)
        termination_date_list.append(termination_date)
        user_status_list.append(user_status)
    # [1] Return matched_index in order of likelihood
    ## [a] If email format matches, take that
    if (user_id.lower() in email_format_list):
        matched_index = email_format_list.index(user_id.lower())
    ## [b] If not, look at user_id in first_initial + last_name
    elif (user_id.lower() in first_initial_last_name_list):
        if first_initial_last_name_list.count(user_id.lower()) > 1:
            if len([i for i in range(len(csvdataRows)) if (user_id.lower()==first_initial_last_name_list[i]) and ((termination_date_list[i]=='None' and user_status_list[i].lower()=='active') or (termination_date_list[i]>datetime.strptime('1/1/21','%m/%d/%y')))])==1:
                matched_index = [i for i in range(len(csvdataRows)) if (user_id.lower()==first_initial_last_name_list[i]) and ((termination_date_list[i]=='None' and user_status_list[i].lower()=='active') or (termination_date_list[i]>datetime.strptime('1/1/21','%m/%d/%y')))][0]
            else:
                print([i for i in range(len(csvdataRows)) if (user_id.lower()==first_initial_last_name_list[i]) and ((termination_date_list[i]=='None' and user_status_list[i].lower()=='active') or (termination_date_list[i]>datetime.strptime('1/1/21','%m/%d/%y')))])
                matched_index='None'#only "brobinson" --> not clear which to me (think brian robinson ie IT, but not sure)
        #for i in range(len(csvdataRows)):
        #    if (user_id.lower()==first_initial_last_name_list[i]) and ((termination_date_list[i]=='None' and user_status_list[i].lower()=='active') or (termination_date_list[i]>datetime.strptime('1/1/21','%m/%d/%y'))):
        #        print(user_id)
        else:
            matched_index = first_initial_last_name_list.index(user_id.lower())
    ## [c] If not, look at first_initial + last_name_multiple_surnames
    elif (user_id.lower() in first_initial_last_name_multiple_surnames_list):
        matched_index = first_initial_last_name_multiple_surnames_list.index(user_id.lower())
    ## [d] If not, look at first_name + last_name
    elif (user_id.lower() in first_name_last_name_list):
        matched_index = first_name_last_name_list.index(user_id.lower())
    ## [n] Check for user_id sans alphanumerics (e.g., user_id="_kbly")
    elif (re.compile('[^a-zA-Z]').sub('', user_id.lower()) in first_initial_last_name_list):
        matched_index = first_initial_last_name_list.index(re.compile('[^a-zA-Z]').sub('', user_id.lower()))
    ## [n+1] Otherwise fuzzy-match? (Off 1 character?)
    ## [n+1] Manual match
    elif (user_id.lower() in manual_match_dict.keys()):
        matched_index = employee_number_list.index(manual_match_dict[user_id.lower()])
    #elif ():
    else:
        matched_index='None'
    return matched_index

matched_indexes = [name_match(user_id, csvdataRows) for user_id in time_series_unique_user_ids]
missing_entries = [time_series_unique_user_ids[i] for i in range(len(time_series_unique_user_ids)) if matched_indexes[i] in ['None',None]]
# From time-series --> active team member sheet
matched_indexes_dict = {time_series_unique_user_ids[i]:matched_indexes[i] for i in range(len(time_series_unique_user_ids)) if matched_indexes[i] not in ['None',None]}




import string


csvFileName = 'regional-management-data/Active Team Member List.csv'
csvdataRows = readCSV(csvFileName)
csvdataRows[0].append('user_id')
for row in csvdataRows[1:]:
    first_name = row[csvdataRows[0].index('\ufeffFirst Name')]
    last_name = row[csvdataRows[0].index('Last Name')]
    # People with multiple last names
    if len(last_name.split())>1:
        last_name = last_name.split()[len(last_name.split())-1]
    row.append(first_name.lower()[0]+last_name.lower())
len([i for i in time_series_unique_user_ids if (i in [row[csvdataRows[0].index('user_id')] for row in csvdataRows[1:]])])/len(time_series_unique_user_ids)#0.8227091633466136
len([i.translate(str.maketrans('', '', string.punctuation)) for i in time_series_unique_user_ids if (i in [row[csvdataRows[0].index('user_id')] for row in csvdataRows[1:]])])/len(time_series_unique_user_ids)#0.8227091633466136
missing_entries = [i for i in time_series_unique_user_ids if (i not in [row[csvdataRows[0].index('user_id')] for row in csvdataRows[1:]])]


################################ End [2] Map training time-series to name, start dates (new hires in {0,1}), branch ################################

################################ [3] Map sales data to time-series + branch data ################################
# The "Employee #2" column in the production data sheet appears to be the same as "Employee number" in the active team member sheet (after checking a few samples) - can you confirm this (why #2?)
import random
for q in range(5):
    i = random.randint(0,len(time_series_unique_user_ids)-1)
    time_series_id = time_series_unique_user_ids[i]
    active_team_member_row = csvdataRows[matched_indexes_dict[time_series_id]]
    employee_number = active_team_member_row[csvdataRows[0].index("Employee Number")]
    # [row[csvdataRowsSales[0].index('Employee Number #2')][len('00'):] for row in csvdataRowsSales].index(employee_number)
    ## Below will have multiple rows - 1 for each month on payroll
    relevant_sales_production_rows = [row for row in csvdataRowsSales if (row[csvdataRowsSales[0].index('Employee Number #2')][len('00'):]==employee_number)]

def build_NNM_quantiles(df):
    # [1] Read in data
    csvFileNameSales = 'regional-management-data/2021 Employee Production Data.csv'
    csvdataRowsSales = readCSV(csvFileNameSales)
    # [2] Remove non-relevant rows (e.g., $0 NNM, nonactive, busdevrep job_title)
    relevant_rows = [row for row in csvdataRowsSales[1:] if (row[csvdataRowsSales[0].index(' Total NNM ')].replace(' ','') not in ['-', 0, '0']) and (row[csvdataRowsSales[0].index('NLS User ID')] not in ['#N/A','N/A']) and (row[csvdataRowsSales[0].index('HR Position')]!='BUSDEVREP') and (row[csvdataRowsSales[0].index('JOB')]!='BUSDEVREP')]
    # [3] Turn $ format into usable floats/ints
    csvdataRowsSales[0].append('actual_sales')
    for row in relevant_rows:
        row.append(float(row[csvdataRowsSales[0].index(' Total NNM ')].replace(',','').replace('-','0')))
    # [4] Turn into dataframe
    import pandas as pd
    df_actual_sales = pd.DataFrame(relevant_rows, columns=csvdataRowsSales[0])
    # [5] Calculate percentiles/quartiles
    NNM_quantiles = {}
    NNM_percentile_categories = [0.0, 0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0]
    for i in range(len(NNM_percentile_categories)-1):
        NNM_quantiles[i] = [df_actual_sales['actual_sales'].quantile(NNM_percentile_categories[i]),df_actual_sales['actual_sales'].quantile(NNM_percentile_categories[i+1])]
    df_actual_sales['actual_sales'].quantile(0.1)
    df_actual_sales['actual_sales'].quantile(0.25)
    df_actual_sales['actual_sales'].quantile(0.5)
    df_actual_sales['actual_sales'].quantile(0.75)
    df_actual_sales['actual_sales'].quantile(0.9)
    # [n] Filter to relevant rows (those in the time_series / user_log data - alternatively... also those not?) [NOT TODAY]
    return NNM_quantiles

def map_time_series_id_to_production_data(time_series_id='carlosgarcia'):
    # Relevant data (see Notes from conversation with Mitch regional management 7/23/31 email for more information)
    # [1] $0 sales (NNM) in a month --> Remove people that have [a] $0 actual sales AND/ OR [b] people that have #NA in NLS User IDfield [DONE]
    # [2] Wildly different targes (e.g., up to 10M) --> use NNM quartiles across entire set (not targets/ratios)
    # [3] Relevant jobs - all but businessdevrep [DONE]
    # [4] New hire column - anyone with 'New Hire' = 0.8,0.6 OR 0.6/0.8 in PRIOR month [i.e., make it 3 months] [DONE]
    # [n] PTO columns? Should we account for this? (e.g., someone takes time off?) [not today]
    csvFileNameSales = 'regional-management-data/2021 Employee Production Data.csv'
    csvdataRowsSales = readCSV(csvFileNameSales)
    from datetime import datetime
    active_team_member_row = csvdataRows[matched_indexes_dict[time_series_id]]
    employee_number = active_team_member_row[csvdataRows[0].index("Employee Number")]
    #relevant_sales_production_rows = [row for row in csvdataRowsSales if (row[csvdataRowsSales[0].index('Employee Number #2')][len('00'):]==employee_number)]
    relevant_sales_production_rows = [row for row in csvdataRowsSales if (row[csvdataRowsSales[0].index('Employee Number #2')].lstrip('0')==employee_number)]
    months = {'Jan':'2021-01-31', 'Feb':'2021-02-28', 'Mar':'2021-03-31', 'Apr':'2021-04-30', 'May':'2021-05-31', 'Jun':'2021-06-30'}
    relevant_sales_rows_for_df = []
    for i in range(len(relevant_sales_production_rows)):
        row = relevant_sales_production_rows[i]
        # [1a] Remove rows with $0 sales (NNM) per month
        if (row[csvdataRowsSales[0].index(' Total NNM ')].replace(' ','') not in ['-', 0, '0']):
            # [1b] Remove people that have #N/A in their 'NLS User ID' field
            if (row[csvdataRowsSales[0].index('NLS User ID')] not in ['#N/A','N/A']):
                # [3] Remove businessdevrep job_title
                if ((row[csvdataRowsSales[0].index('HR Position')]!='BUSDEVREP') and (row[csvdataRowsSales[0].index('JOB')]!='BUSDEVREP')):
                    # [4] Classify new hires (0.8, 0.6 OR same in previous month --> count for 3 months) [ASSUMES ROWS IN ORDER AND MONTHLY]
                    new_hire_status = True if ((row[csvdataRowsSales[0].index('New Hire')] in ['0.6', '0.8']) or (relevant_sales_production_rows[i-1][csvdataRowsSales[0].index('New Hire')] in ['0.6', '0.8'] if (i!=0 and i<len(relevant_sales_production_rows)) else False)) else False
                    # [n] Is this (below) still necessary? [need to check]
                    #if ('#N/A' not in row) and ('#DIV/0!' not in row):#
                    username = time_series_id
                    dt_string = months[[key for key in list(months.keys()) if key.lower() in row[csvdataRowsSales[0].index('Month')].lower()][0]]
                    dt_dtobject = datetime.strptime(dt_string, '%Y-%m-%d')
                    actual_sales = float(row[csvdataRowsSales[0].index(' Total NNM ')].replace(',','').replace('-','0'))
                    actual_sales_quartile = [key for key in list(NNM_quantiles.keys()) if actual_sales>=NNM_quantiles[key][0] and actual_sales<NNM_quantiles[key][1]][0]
                    job_title = row[csvdataRowsSales[0].index('JOB')]
                    employee_number = row[csvdataRowsSales[0].index('Employee Number #2')].lstrip('0')
                    relevant_sales_rows_for_df.append([username, dt_dtobject, actual_sales, actual_sales_quartile, new_hire_status, job_title, employee_number])
                    #sales_target = float(row[csvdataRowsSales[0].index('NNM Standard')].replace(',','').replace('-','0'))
                    #sales_percentage_of_target_achieved = float(row[csvdataRowsSales[0].index(' % NNM Target ')].replace(',','').replace('%','').replace('-','0'))/100
                    #relevant_rows_for_df.append([username, dt_dtobject, actual_sales, sales_target, sales_percentage_of_target_achieved])
    return relevant_sales_rows_for_df

def build_production_dataframe(df_training, matched_indexes_dict):
    csvdataRowsProductionDF = [['username', 'dt_dtobject', 'actual_sales', 'actual_sales_quartile', 'new_hire_status', 'job_title', 'employee_number']]
    for time_series_id in df_training['username'].unique():
        if time_series_id in list(matched_indexes_dict.keys()):
            relevant_rows_for_df = map_time_series_id_to_production_data(time_series_id)
            csvdataRowsProductionDF += relevant_rows_for_df
    # DF
    import pandas as pd
    df_production = pd.DataFrame(csvdataRowsProductionDF[1:], columns=csvdataRowsProductionDF[0])
    return df_production

# NEED TO MERGE [a] time_series usage/month and [b] sales/month





for username in high_activity_users:
    print(username)
    monthly = ts[ts['username'].isin([username])].groupby(pd.Grouper(freq='M'))
    monthly.count()
    print(' ')


# Average % sales target for high_activity_users vs. average % sales target for low_activity_users
high_activity_users = [username for username in list(username_count_dict.keys()) if username_count_dict[username]>(mean(list(username_count_dict.values()))+1*stdev(list(username_count_dict.values())))]
low_activity_users = [username for username in list(username_count_dict.keys()) if username_count_dict[username]<=(mean(list(username_count_dict.values()))+1*stdev(list(username_count_dict.values())))]

unique_usernames_production = df_production['username'].unique()
ts_production = df_production.set_index('dt_dtobject')

for username in high_activity_users:
    if username in unique_usernames_production:
        monthly = ts_production[ts_production['username'].isin([username])].groupby(pd.Grouper(freq='M'))
        monthly.mean()

monthly = ts[ts['username'].isin(['mwettschurack'])].groupby(pd.Grouper(freq='M'))
################################ End [3] Map sales data to time-series + branch data ################################

def build_activeteammember_to_training_map():
    #### MAPS <ALL> EMPLOYEES IN ACTIVE TEAM MEMBER LIST TO TRAINING LOGS --> {'employee number':'username'} ####
    ### ALL IS IMPORTANT BECAUSE IT INCLUDES THOSE THAT DON'T ACTUALLY SHOW UP IN THE TRAINING LOGS --> MAPPED TO None
    # [1] Read in active team member list
    csvFileNameActiveTeamMemberList = 'regional-management-data/Active Team Member List.csv'
    csvdataRowsActiveTeamMemberList = readCSV(csvFileNameActiveTeamMemberList)
    # [2] Build inverted matched_indexes_dict
    activeteammember_to_training_map = {}
    matched_indexes_dict_inverted = {v: k for k, v in matched_indexes_dict.items()}
    for i in range(len(csvdataRowsActiveTeamMemberList)):
        row = csvdataRowsActiveTeamMemberList[i]
        employee_number = row[csvdataRowsActiveTeamMemberList[0].index('Employee Number')]
        if i in list(matched_indexes_dict_inverted.keys()):
            activeteammember_to_training_map[employee_number] = matched_indexes_dict_inverted[i]
        else:
            activeteammember_to_training_map[employee_number] = None
    return activeteammember_to_training_map
# unidirectional (more one way than the other [the other; e.g., CEO <> X])
"""
def build_employee_number_to_username_map():
    #### MAPS ALL EMPLOYEES IN PRODUCTION_DATA TO A USER_ID OR NONE (IF NO MATCH IN TRAINING --> 0 USE) ####
    csvFileNameProduction = 'regional-management-data/2021 Employee Production Data.csv'
    csvFileNameProduction = readCSV(csvFileNameSales)
    # For each row in production
    for row in csvFileNameProduction[1:]:
        #employee_number = row[csvFileNameProduction[0].index('Employee Number #2')][len('00'):]
        employee_number = row[csvFileNameProduction[0].index('Employee Number #2')].lstrip('0')
        if employee_number not in list(activeteammember_to_training_map.keys()):
            print(employee_number)
        # Check if employee number has a match in training data
    # ONLY 7 new employees missing from data set [7525, 7526, 7527, 7528, 7529, 7530, 7531]




    active_team_member_row = csvdataRows[matched_indexes_dict[time_series_id]]
    employee_number = active_team_member_row[csvdataRows[0].index("Employee Number")]
    return
"""

def merge_training_and_sales_data(df_training_dictionary_strings, activeteammember_to_training_map, df_production):
    #### DOES NOT CURRENTLY ACCOUNT FOR BLANK DATA - E.G., IF USERNAME USES IN JAN = 0 (no rows) ####
    # START WITH EMPLOYEE NUMBER IN PRODUCTION LIST #
    # Needs to start with production --> anyone who is not listed = 0...?
    # ('username', 'month') - e.g., ('ywaller', '2021-01-31')
    # [0] Create merged_rows
    merged_rows = [['Username', 'Month (DT)', 'Employee number', 'Job title', 'District', 'State', 'New hire status', 'Total NNM (sales)', 'NNM (sales) quintile', 'Training count (number of instances)']]
    # [1] Read in production (sales) list
    csvFileNameProduction = 'regional-management-data/2021 Employee Production Data.csv'
    csvdataRowsProduction = readCSV(csvFileNameProduction)
    unique_employee_numbers_production = list(set([row[csvdataRowsProduction[0].index('Employee Number #2')].lstrip('0') for row in csvdataRowsProduction[1:]]))#1520
    # [2] Define month map (to be used later to map training to sales)
    months = {'Jan':'2021-01-31', 'Feb':'2021-02-28', 'Mar':'2021-03-31', 'Apr':'2021-04-30', 'May':'2021-05-31', 'Jun':'2021-06-30'}
    # [3] For each unique employee (number), grab all relevant rows (important for determining new hire status)
    for employee_number in unique_employee_numbers_production:
        # Grab relevant rows
        relevant_rows = [row for row in csvdataRowsProduction if (row[csvdataRowsProduction[0].index('Employee Number #2')].lstrip('0')==employee_number)]
        for row in relevant_rows:
            # [4] Discard non-relevant rows (e.g., $0 sales)
            # [4a] Remove rows with $0 sales (NNM) per month
            if (row[csvdataRowsProduction[0].index(' Total NNM ')].replace(' ','') not in ['-', 0, '0']):
                # [4b] Remove people that have #N/A in their 'NLS User ID' field
                if (row[csvdataRowsProduction[0].index('NLS User ID')] not in ['#N/A','N/A']):
                    # [4c] Remove businessdevrep job_title
                    if ((row[csvdataRowsProduction[0].index('HR Position')]!='BUSDEVREP') and (row[csvdataRowsProduction[0].index('JOB')]!='BUSDEVREP')):
                        # [5] Classify new hires (0.8, 0.6 OR same in previous month --> count for 3 months) [ASSUMES ROWS IN ORDER AND MONTHLY]
                        new_hire_status = True if ((row[csvdataRowsProduction[0].index('New Hire')] in ['0.6', '0.8']) or (relevant_rows[i-1][csvdataRowsProduction[0].index('New Hire')] in ['0.6', '0.8'] if (i!=0 and i<len(relevant_rows)) else False)) else False
                        # [6] If employee_count in activeteammember_to_training_map
                        total_nnm_sales = float(row[csvdataRowsProduction[0].index(' Total NNM ')].replace(',','').replace('-','0'))
                        nnm_sales_quintile = [key for key in list(NNM_quantiles.keys()) if total_nnm_sales>=NNM_quantiles[key][0] and total_nnm_sales<NNM_quantiles[key][1]][0] if len([key for key in list(NNM_quantiles.keys()) if total_nnm_sales>=NNM_quantiles[key][0] and total_nnm_sales<NNM_quantiles[key][1]])>0 else (max(list(NNM_quantiles.keys())) if total_nnm_sales>=NNM_quantiles[max(list(NNM_quantiles.keys()))][1] else 0)
                        job_title = row[csvdataRowsProduction[0].index('HR Position')]
                        dt_string = months[[key for key in list(months.keys()) if key.lower() in row[csvdataRowsProduction[0].index('Month')].lower()][0]]
                        district = row[csvdataRowsProduction[0].index('District')]
                        state = row[csvdataRowsProduction[0].index('State')]
                        # dt_dtobject = datetime.strptime(dt_string, '%Y-%m-%d')
                        # username
                        # training_count
                        # Find username and check if (username, dt_string) pair in training logs
                        if employee_number in list(activeteammember_to_training_map.keys()):
                            username = activeteammember_to_training_map[employee_number]
                            if ((username, dt_string) in (df_training_dictionary_strings.keys())):
                                training_count = df_training_dictionary_strings[(username, dt_string)]
                            else:
                                training_count = 0
                            # Append
                            merged_rows.append([username,dt_string,employee_number,job_title,district,state,new_hire_status,total_nnm_sales,nnm_sales_quintile,training_count])
                        # Otherwise (if employee number doesn't even exist, set training count to 0)
                        else:
                            # Don't add anything (could add username=None?)
                            print('Missing employee #: '+ employee_number)
                            training_count = 0
    return merged_rows

def perform_merged_analysis(merged_rows):
    # [0] Transform rows into dataframe
    import pandas as pd
    merged_df = pd.DataFrame(merged_rows[1:], columns=merged_rows[0])
    # [1] Who is using the software/tools?
    #### May want to divide sum by count - e.g., for new hires, they are only 5% of the population, but represent 15% of the total use ####
    ## [a] By job title
    job_title_sum_dict = {key:dict(merged_df.groupby(['Job title']).sum())['Training count (number of instances)'][key] for key in dict(merged_df.groupby(['Job title']).sum())['Training count (number of instances)'].keys()}
    job_title_count_dict = {key:dict(merged_df.groupby(['Job title']).count())['Training count (number of instances)'][key] for key in dict(merged_df.groupby(['Job title']).count())['Training count (number of instances)'].keys()}
    job_title_weighted_sum_dict = {key:job_title_sum_dict[key]/job_title_count_dict[key] for key in job_title_sum_dict.keys()}
    # job_title_weighted_sum_dict = {'ASM': 0.4712546020957236, 'BOIPT': 1.1724137931034482, 'CSR': 0.4214792299898683, 'MGR': 0.9079110012360939, 'PTCSR': 0.34513274336283184}
    #merged_df.groupby(['Job title']).sum()#
    ## [b] New hires vs. experienced hires
    new_hire_sum_dict = {key:dict(merged_df.groupby(['New hire status']).sum())['Training count (number of instances)'][key] for key in dict(merged_df.groupby(['New hire status']).sum())['Training count (number of instances)'].keys()}
    new_hire_title_count_dict = {key:dict(merged_df.groupby(['New hire status']).count())['Training count (number of instances)'][key] for key in dict(merged_df.groupby(['New hire status']).count())['Training count (number of instances)'].keys()}
    new_hire_weighted_sum_dict = {key:new_hire_sum_dict[key]/new_hire_title_count_dict[key] for key in new_hire_sum_dict.keys()}
    # new_hire_weighted_sum_dict = {False: 0.520767306088407, True: 1.6656891495601174}
    # merged_df.groupby(['New hire status']).sum()
    ## [c] Geography (state, district)
    #### [ci] District
    district_sum_dict = {key:dict(merged_df.groupby(['District']).sum())['Training count (number of instances)'][key] for key in dict(merged_df.groupby(['District']).sum())['Training count (number of instances)'].keys()}
    district_title_count_dict = {key:dict(merged_df.groupby(['District']).count())['Training count (number of instances)'][key] for key in dict(merged_df.groupby(['District']).count())['Training count (number of instances)'].keys()}
    district_weighted_sum_dict = {key:district_sum_dict[key]/district_title_count_dict[key] for key in district_sum_dict.keys()}
    # district_weighted_sum_dict = {'AL1': 0.8308823529411765, 'AL2': 0.3671875, 'AL3': 1.865546218487395, 'AL5': 0.6370370370370371, 'AL6': 0.8011695906432749, 'GA1': 0.8011049723756906, 'IL1': 0.3333333333333333, 'MO1': 0.7134146341463414, 'MO2': 0.8787878787878788, 'NC1': 0.43646408839779005, 'NC2': 0.39215686274509803, 'NC3': 0.5251396648044693, 'NC4': 0.4890829694323144, 'NM1': 1.0098039215686274, 'NM2': 0.38095238095238093, 'OK1': 1.1401869158878504, 'OK2': 0.5073529411764706, 'OK3': 0.5620437956204379, 'SC1': 1.2340425531914894, 'SC2': 0.5824175824175825, 'SC3': 0.17297297297297298, 'SC4': 0.35802469135802467, 'SC5': 0.5953757225433526, 'SC6': 0.4451219512195122, 'TN1': 0.41964285714285715, 'TN2': 0.576271186440678, 'TN3': 0.6120689655172413, 'TX1': 0.13402061855670103, 'TX10': 0.4512820512820513, 'TX11': 0.45774647887323944, 'TX12': 0.5885714285714285, 'TX2': 0.5615942028985508, 'TX3': 0.5757575757575758, 'TX5': 0.6174863387978142, 'TX6': 0.15168539325842698, 'TX7': 0.577639751552795, 'TX8': 0.2679738562091503, 'TX9': 0.38202247191011235, 'VA1': 1.0545454545454545, 'VA2': 0.6, 'WI1': 0.5}
    #csvFileNameToSave = 'district.csv'
    #csvdataRowsToSave = [[key,district_weighted_sum_dict[key]] for key in district_weighted_sum_dict.keys()]
    #writeToCSV(csvFileNameToSave, csvdataRowsToSave)
    #### [cii] State
    state_sum_dict = {key:dict(merged_df.groupby(['State']).sum())['Training count (number of instances)'][key] for key in dict(merged_df.groupby(['State']).sum())['Training count (number of instances)'].keys()}
    state_title_count_dict = {key:dict(merged_df.groupby(['State']).count())['Training count (number of instances)'][key] for key in dict(merged_df.groupby(['State']).count())['Training count (number of instances)'].keys()}
    state_weighted_sum_dict = {key:state_sum_dict[key]/state_title_count_dict[key] for key in state_sum_dict.keys()}
    #csvFileNameToSave = 'state.csv'
    #csvdataRowsToSave = [[key,state_weighted_sum_dict[key]] for key in state_weighted_sum_dict.keys()]
    #writeToCSV(csvFileNameToSave, csvdataRowsToSave)
    # state_weighted_sum_dict = {'AL': 0.8780841799709724, 'GA': 0.8011049723756906, 'IL': 0.3333333333333333, 'MO': 0.7871621621621622, 'NC': 0.46495956873315364, 'NM': 0.6622807017543859, 'OK': 0.7052631578947368, 'SC': 0.5422045680238332, 'TN': 0.5375722543352601, 'TX': 0.437, 'VA': 0.8542372881355932, 'WI': 0.5}
    #merged_df.groupby(['District']).sum()#
    #merged_df.groupby(['State']).sum() #
    # me: what happens when we account for # of people? more generally, is this concentrated in a small # of users or very diverse?
    #merged_df.groupby(['Username']).count().values.tolist()
    # EVEN AMONG NEW HIRES, LOTS OF VARIATION OF USE...
    from statistics import mean, stdev
    merged_df[merged_df['New hire status'] == True].groupby(['Username']).sum()['Training count (number of instances)']
    print(mean(merged_df[merged_df['New hire status'] == True].groupby(['Username']).sum()['Training count (number of instances)'].values))#4
    print(stdev(merged_df[merged_df['New hire status'] == True].groupby(['Username']).sum()['Training count (number of instances)'].values))#11.445523142259598
    # [2] Sales/use correlation [are high performers more likely to use?]
    merged_df['NNM (sales) quintile'].corr(merged_df['Training count (number of instances)'])#-0.093208085094307... loose negative correlation --> my assumption would be that worse people are using it
    merged_df['Total NNM (sales)'].corr(merged_df['Training count (number of instances)'])#-0.0873832496753009
    ## Within new hires?
    merged_df[merged_df['New hire status'] == True]['NNM (sales) quintile'].corr(merged_df[merged_df['New hire status'] == True]['Training count (number of instances)'])#-0.15804574343206235
    merged_df[merged_df['New hire status'] == True]['Total NNM (sales)'].corr(merged_df[merged_df['New hire status'] == True]['Training count (number of instances)'])#-0.14035738506642836
    ## merged_df[merged_df['New hire status'] == True]['NNM (sales) quintile'].corr(merged_df[merged_df['New hire status'] == True]['Training count (number of instances)'])#-0.15804574343206235
    ## merged_df[merged_df['New hire status'] == True]['Total NNM (sales)'].corr(merged_df[merged_df['New hire status'] == True]['Training count (number of instances)'])#-0.14035738506642836
    ## Is this selection bias? i.e., people doing worse job NEED to watch more videos? OR people worse people need help?
    ## Can we predict who needs help based off how many videos they're watching...?
    ## THIS: Do the videos appear to help? E.g., if a new_hire is using it, do they get better in the next month?
    # Build lagged time-series
    rows = merged_df[merged_df['New hire status'] == True][['Username', 'NNM (sales) quintile', 'Training count (number of instances)']].values.tolist()
    unique_employee_ids = list(set([row[0] for row in rows]))
    lagged_time_series = [['Username', 'NNM (sales) quintile', 'Training count (number of instances)']]
    for unique_employee_id in unique_employee_ids:
        if (unique_employee_id!=None):
            relevant_rows = [row for row in rows if row[0]==unique_employee_id]
            if len(relevant_rows)>0:
                for i in range(len(relevant_rows)-1):
                    username = unique_employee_id
                    nmm_sales_quintile = relevant_rows[i+1][1]
                    training_count = relevant_rows[i][2]
                    lagged_time_series.append([username, nmm_sales_quintile, training_count])
    lagged_df = pd.DataFrame(lagged_time_series[1:], columns=lagged_time_series[0])
    lagged_df['NNM (sales) quintile'].corr(lagged_df['Training count (number of instances)'])#-0.2327749021417048

    import matplotlib.pyplot as plt
    x=lagged_df['NNM (sales) quintile']
    y=lagged_df['Training count (number of instances)']
    plt.scatter(x, y)
    plt.xlabel('NNM (sales) quintile t+1')
    plt.ylabel('New hiresraining count (number of instances) t')
    #plt.show()
    plt.title("New hires lagged (correlation = -0.23)")
    plt.savefig('scatter_sales_nh_lagged.png')
    plt.close()

    # [3] Timing - when are they using it (only 1st month)?
    # [4] Specific videos
    return

# Story
## When looking in aggregate, hides reality --> start looking at it in weighted sum (sum/count)
## Jobs - it is more used by BOIPT than others (makes sense, younger etc.)
## Reasonably significant variation among branch, district and new hires
## New hires do use significantly more than experienced
## Usage appears inversely correlated to sales output - but is this just selection bias (worse salespeople need more help?)
## In order to attempt to root this out, look at over time performance













    merged_rows = [['Username', 'Month (DT)', 'Employee number', 'Job title', 'New hire status', 'Total NNM (sales)', 'NNM (sales) quintile', 'Training count (number of instances)']]
    from datetime import datetime
    df_training_keys = [(i[0], i[1].strftime("%Y-%m-%d")) for i in list(df_training_dictionary['url'].keys())]
    df_production_keys = [(i[0],i[1].strftime("%Y-%m-%d")) for i in df_production.values.tolist()]
    for row in df_production.values.tolist():
        production_key = (row[0],row[1].strftime("%Y-%m-%d"))
        username = row[0]
        dt_string = row[1].strftime("%Y-%m-%d")
        total_nnm_sales = row[2]
        nnm_sales_quintile = row[3]
        new_hire_status = row[4]
        job_title = row[5]
        employee_number = row[6]
        if production_key in df_training_keys:
            training_count = int(df_training_dictionary['url'][production_key])
        else:
            training_count = 0
        new_row = [username, dt_string, employee_number, job_title, new_hire_status, total_nnm_sales, nnm_sales_quintile, training_count]
        merged_rows.append(new_row)
    # Transform rows into dataframe
    return merged_df

# Analyses
# [0] Include all people in employee_production_map OR xyz (even if not in logs - e.g., don't use computer...? what % of people is this?) [DONE]
# [1] Who is using the software/tools? []
# [2] When are they using it?
# [3] Are specific videos being viewed more than others?
# [4] $sales vs. training use
