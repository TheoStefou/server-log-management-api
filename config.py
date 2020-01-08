import pymongo

client = pymongo.MongoClient('mongodb://localhost:27017/')

db = client['nosql']
logs_col = db['logs']
admins_col = db['admins']