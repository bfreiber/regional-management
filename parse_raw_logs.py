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

def build_time_series(structured_data):
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
df = build_data_frame(relevant_rows)

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
