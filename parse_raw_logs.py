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
