import pymongo

from config import logs_col, admins_col
from bson.son import SON
from bson.objectid import ObjectId

'''
1. Find the total logs per type that were created within a specified time range and sort them in
a descending order. Please note that individual files may log actions of more than one type.
'''
def query1(fr, to):

    return list(logs_col.aggregate(
            [
                {'$match': {'timestamp': {'$gte': fr, '$lte': to} } },
                {'$group':{ '_id':'$type', 'count': {'$sum': 1} } },
                {'$sort': SON({'count': -1 }) },
                {'$project': {'_id': 0, 'type': '$_id', 'count': 1}}
            ]
        )
    )

'''
2. Find the number of total requests per day for a specific log type and time range.
'''
def query2(fr, to, typ):

    return list(logs_col.aggregate(
            [
                {'$match': {'timestamp': {'$gte': fr, '$lte': to}, 'type': typ } },
                {'$group':
                    {
                        '_id': {
                            '$dateToString' : {
                                'format': '%d-%m-%Y',
                                'date': '$timestamp'
                            }
                            
                        },
                        'count': {'$sum': 1}
                    }
                },
                {'$project': {'_id': 0, 'day': '$_id', 'count': 1}}
            ]
        )
    )

'''
3. Find the three most common logs per source IP for a specific day.
'''
def query3(fr, to):

    return list(logs_col.aggregate(
            [
                {'$match': {'timestamp': {'$gte': fr, '$lte': to} } },
                {'$group':
                    {
                        '_id': {
                            'source_ip': '$source_ip',
                            'type': '$type'
                        },
                        'numtypes': {'$sum': 1}
                    }
                },
                {'$project': {'_id': 0, 'source_ip': '$_id.source_ip', 'type': '$_id.type', 'numtypes': 1}},
                {'$group':
                    {
                        '_id': '$source_ip',
                        'countpertype': {
                            '$push': {
                                'type': '$type',
                                'count': '$numtypes'
                            }
                        }
                    }
                },
                {'$unwind': '$countpertype'},
                {'$sort': {'countpertype.count': -1} },
                {'$group':
                    {
                        '_id': '$_id',
                        'sortedArr': {'$push': {'type': '$countpertype.type', 'count': '$countpertype.count'}}
                    }

                },
                {'$project': {'_id':0, 'source_ip': '$_id', 'types': {'$slice': ['$sortedArr', 3]}} }
                
            ]
        )
    )

'''
4. Find the two least common HTTP methods with regards to a given time range.
'''
def query4(fr, to):

    return list(logs_col.aggregate(
            [
                {'$match': {'timestamp': {'$gte': fr, '$lte': to}, 'http_method': {'$exists': True} }  },
                {'$group':{ '_id':'$http_method', 'count': {'$sum': 1} } },
                {'$sort' : SON({'count': 1 })},
                {'$limit': 2},
                {'$project': { '_id': 0, 'http_method': '$_id'} }
            ]
        )
    )

'''
5. Find the referers (if any) that have led to more than one resources
'''
def query5():

     return list(logs_col.aggregate(
            [
                {'$match': {'referer': {'$exists': True}, 'resource': {'$exists': True}  }  },
                {
                    '$group': 
                    {
                        '_id':'$referer', 'resources': {'$addToSet': '$resource'}
                    }
                },
                {'$project': {'_id':0, 'referer': '$_id', 'arrsize': {'$size': '$resources'}}},
                {'$match' : { 'arrsize': {'$gte': 2} } },
                {'$project': {'referer': 1}}
            ]
        )
    )
    
'''
6. Find the blocks that have been replicated the same day that they have also been served.
'''
def query6():

    return list(logs_col.aggregate(
            [
                {'$match': { '$or': [{'type': 'replicate'}, {'type': 'served'}] }  },
                {'$project': {'timestamp': 1, 'type': 1, 'block_ids': 1}},
                {'$unwind' : '$block_ids'},
                {
                    '$group' :
                    {
                        '_id': {    
                            'day' : {
                                        '$dateToString' : {
                                        'format': '%d-%m-%Y',
                                        'date': '$timestamp'
                                    }
                            },
                            'block': '$block_ids', 'type': '$type' }
                    }
                },
                {'$project': {'day': '$_id.day', 'block': '$_id.block', 'type': '$_id.type'}},
                {
                    '$group' :
                    {
                        '_id': {'day': '$day', 'block': '$block'},
                        'count': {'$sum': 1}
                    }
                },
                {'$match': {'count': 2}},
                {'$project': {'block': '$_id.block', '_id': 0}}
            ]
        )
    )

'''
7. Find the fifty most upvoted logs for a specific day.
'''
def query7(fr, to):

    return list(logs_col.aggregate(
            [
                {'$match': {'timestamp': {'$gte': fr, '$lte': to} } },
                {
                    '$lookup' :
                    {
                        'from': 'admins',
                        'localField': '_id',
                        'foreignField': 'upvotes',
                        'as': 'upvotes'
                    }
                },
                {'$project': {'votes': {'$size': '$upvotes'}, '_id': 0, 'logid': '$_id'} },
                {'$sort': SON({'votes': -1 })},
                {'$limit': 50}
            ]
        )
    )



'''
8. Find the fifty most active administrators, with regard to the total number of upvotes.
'''
def query8():

    return list(admins_col.aggregate(
            [
                {'$project': {'_id':0, 'username': 1, 'numupvotes': {'$size': '$upvotes'}}},
                {'$sort': SON({'numupvotes': -1 }) },
                {'$limit': 50}
            ]
        )
    )

'''
9. Find the top fifty administrators, with regard to the total number of source IPs for which
they have upvoted logs.
'''
def query9():

    return list(admins_col.aggregate(
            [
                {
                    '$lookup' :
                    {
                        'from': 'logs',
                        'localField': 'upvotes',
                        'foreignField': '_id',
                        #this also replaces the upvotes array that only has ids with the full logs. saving some space
                        'as': 'upvotes'
                    }
                },
                {'$unwind': '$upvotes'},
                {
                    '$group':
                    {
                        '_id': '$username',
                        'source_ips': {'$addToSet': '$upvotes.source_ip'}
                    }
                },
                {'$project': {'username': '$_id', '_id':0, 'source_ips': {'$size': '$source_ips'}}},
                {'$sort': SON({'source_ips': -1 }) },
                {'$limit': 50}

            ]
        )
    )

'''
10. Find all logs for which the same e-mail has been used for more than one usernames when
casting an upvote.
'''
def query10():


    return list(admins_col.aggregate(
            [
                {'$unwind': '$upvotes'},
                {
                    '$group':
                    {
                        '_id': {'logid': '$upvotes', 'email': '$email'},
                        'usernames': {'$addToSet': '$username'}
                    }
                },
                {'$project': {'size': {'$size': '$usernames'}, '_id':1 } },
                {'$match': {'size': {'$gte': 2}}},
                {'$project': {'_id': 0, 'logid': '$_id.logid'}},
                {
                    '$group':
                    {
                        '_id': '$logid'
                    }
                },
                {'$project': {'_id': 0, 'logid': '$_id'}}
            ],
            allowDiskUse=True
        )
    )

'''
11. Find all the block ids for which a given name has casted a vote for a log involving it.
'''
def query11(username):

    return list(admins_col.aggregate(
            [
                {'$match': {'username': username}},
                {
                    '$lookup' :
                    {
                        'from': 'logs',
                        'localField': 'upvotes',
                        'foreignField': '_id',
                        #this also replaces the upvotes array that only has ids with the full logs. saving some space
                        'as': 'upvotes'
                    }
                },
                {'$unwind': '$upvotes'},
                {'$match': {'upvotes.block_ids': {'$exists': True}}},
                {'$unwind': '$upvotes.block_ids'},
                {
                    '$group':
                    {
                        '_id': '$username',
                        'blocks': {'$addToSet': '$upvotes.block_ids' }
                    }
                },
                {'$project': {'username': '$_id', '_id': 0, 'blocks': 1 }}
            ]
        )
    )
    
'''
Insert a new log. All checks must have been done before calling this function.
Return the inserted log with its _id.
'''
def newlog(log):

    _id = logs_col.insert_one(log).inserted_id
    log['_id'] = _id
    return log

'''
Casting of upvotes. In case the same administrator casts a vote for the
same log a second time, the vote should be rejected.
'''
def newupvote(admin_id, log_id):

    admin_id = ObjectId(admin_id)
    log_id = ObjectId(log_id)
    
    return admins_col.update_one(
        {'_id': admin_id},
        {'$addToSet': {'upvotes': log_id} }
    )


'''
Returns the information of an admin given their username.
'''
def getadminbyusername(username):

    return list(
        admins_col.find({'username': username})
    )