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
[el[0] for el in csvdataRows[1:] if '.mp4' in el[0] and 'w|rmc\\' not in el[0]]

from datetime import datetime
parsed_vector = [{'datetime': datetime.strptime(row[:len('2021-05-03 16:39:13')], '%Y-%m-%d %H:%M:%S'), 'username':row[row.index('w|rmc\\')+len('w|rmc\\'):row.index(' ', row.index('w|rmc\\')+len('w|rmc\\'))], 'url':row[row.index('/'):row.index(' ', row.index('/'))]} for row in rows_with_usernames]
# Questions
## What are those with .mp4 in title + no rmc [username]? Can we tie these back to users (previous action similar url structure?)
## Two formats for people watching videos?
