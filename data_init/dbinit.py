import csv
import pymongo
import datetime
import sys

client = pymongo.MongoClient('mongodb://localhost:27017/')

db = client['nosql']
logs_col = db['logs']
admins_col = db['admins']

#If you do not have enough memory, you may use the logs array as a buffer and call insert_many
#after every m entries
print('Starting access...')
with open('./cleaned_logs/data/access_log.csv') as access_csv:

    access_reader = csv.reader(access_csv, delimiter=',')
    logs = []
    for row in access_reader:

        d = {}
        d['source_ip'] = row[0]
        if row[1] != '-':
            d['user_id'] = row[1]
        d['timestamp'] = datetime.datetime.strptime(row[2], '%d/%b/%Y:%H:%M:%S')
        d['http_method'] = row[3]
        d['resource'] = row[4]
        d['response_status'] = int(row[5])
        d['size'] = int(row[6])
        if row[7] != '-':
            d['referer'] = row[7]
        d['user_agent_string'] = row[8]
        d['type'] = 'access'

        logs.append(d)

    logs_col.insert_many(logs)
print('Finished access...')


print('Starting dataxceiver...')
with open('./cleaned_logs/data/dataxceiver_log.csv') as dataxceiver_csv:

    dataxceiver_reader = csv.reader(dataxceiver_csv, delimiter=',')
    logs = []
    for index, row in enumerate(dataxceiver_reader):

        d = {}
        d['timestamp'] = datetime.datetime.strptime(row[0], '%d%m%y%H%M%S')
        d['block_ids'] = [row[1]]
        d['source_ip'] = row[2]
        d['dest_ips'] = [row[3]]
        if row[4] != '-1':
            d['size'] = int(row[4])
        d['type'] = row[5]

        logs.append(d)
    
    logs_col.insert_many(logs)
print('Finished dataxceiver...')

print('Starting namesystem...')
with open('./cleaned_logs/data/HDFS_FS_Namesystem_log.csv') as namesystem_csv:

    namesystem_reader = csv.reader(namesystem_csv, delimiter=',')
    logs = []
    for row in namesystem_reader:

        d = {}
        d['timestamp'] = datetime.datetime.strptime(row[0], '%d%m%y%H%M%S')
        d['block_ids'] = [i for i in row[1].split()]
        d['source_ip'] = row[2]
        d['dest_ips'] = [i for i in row[3].split()]
        if d['dest_ips'][0] == '-':
            del d['dest_ips']
        d['type'] = row[4]

        logs.append(d)

    logs_col.insert_many(logs)
print('Finished namesystem...')

print('Starting admins...')
print('Loading all log ids...')
log_ids = []
for log in logs_col.find():
    log_ids.append(log['_id'])
print('Log ids loaded...')
with open('./admins.csv') as admins_csv:

    admins_reader = csv.reader(admins_csv, delimiter=',')
    admins = []
    for row in admins_reader:

        d = {}
        d['username'] = row[0]
        d['email'] = row[1]
        d['telephone'] = row[2]
        d['upvotes'] = [int(i) for i in row[3].split()]
        d['upvotes'] = list(map(lambda x: log_ids[x], d['upvotes']))

        admins.append(d)

    admins_col.insert_many(admins)
print('Finished admins...')

print('Everything ok!')