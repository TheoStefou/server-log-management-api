import datetime

__datetimeformat__ = '%d-%m-%Y %H:%M:%S'
__dateformat__ = '%d-%m-%Y'

#As found in https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods
__HTTP_METHODS__ = {'GET', 'HEAD', 'POST', 'PUT', 'DELETE', 'CONNECT', 'OPTIONS', 'TRACE', 'PATCH'}



#----------------------VALIDATE LOG ARGUMENTS----------------------------------
def is_valid_http_method(method):

    return method in __HTTP_METHODS__

'''
A valid ip is a string of 4 integers in [0,255] split with dots.
'''
def is_valid_ip(s):
    t = s.split('.')
    if len(t) != 4:
        return False
    
    for item in t:
        try:
            i = int(item)
        except:
            return False

        if i < 0 or i > 255:
            return False

    return True



#----------------------VALIDATE TIMESTAMPS-------------------------------------
def strtodatetime(s):
    try:
        return datetime.datetime.strptime(s, __datetimeformat__)
    except:
        raise ValueError('Datetime format is: %d-%m-%Y %H:%M:%S')

'''
This function receives a date in __dateformat__ format
and returns two datetimes with the same date but appended with
00:00:00 and 23:59:59 to cover the whole day with datetime objects
'''
def date_to_fromto(s):
    try:
        datetime.datetime.strptime(s, __dateformat__)
    except:
        raise ValueError('Date format is: %d-%m-%Y')

    return strtodatetime(s+' 00:00:00'), strtodatetime(s+' 23:59:59')


#----------------------VALIDATE NEW LOG----------------------------------------
def validate_log(log):

    try:
        typ = log['type']
    except:
        raise ValueError('Missing type.')

    if not isinstance(typ, str):
        raise ValueError('Type must be a string.')

    if typ == 'access':
        validate_access(log)
    elif typ == 'receiving' or typ =='received' or typ == 'served':
        validate_dataxceiver(log)
    elif typ == 'replicate' or typ == 'delete':
        validate_namesystem(log)
    else:
        raise ValueError('Type is not one of (access)->access, (receiving, received, served)->dataxceiver, (replicate, delete)->HDFS_FS_Namesystem. Instead found: '+typ)


'''
For the following four validate functions, start with the number of expected args. If an arguments
is found missing, if it is not required, subtract 1, otherwise throw an exception. Lastly, check
if the length of the dictionary/json is the same as the expected args, in case there are extra
arguments that we do not want to add to the database. (a bad key)
Also, for every argument, require its data type to be the one expected using the isinstance function.
'''
def validate_access(log):
    expected_args = 10
    
    if 'source_ip' not in log:
        raise ValueError('Missing source ip.')
    else:
        if not isinstance(log['source_ip'], str):
            raise ValuerError('Source ip must be a string.')
        if not is_valid_ip(log['source_ip']):
            raise ValueError('Wrong source ip. Must be: [0-255].[0-255].[0-255].[0-255]')
    
    if 'user_id' not in log:
        expected_args -= 1
    else:
        if not isinstance(log['user_id'], str):
            raise ValueError('User id must be a string.')

    if 'timestamp' not in log:
        raise ValueError('Missing timestamp.')
    else:
        if not isinstance(log['timestamp'], str):
            raise ValuerError('Timestamp must be a string with format: %d-%m-%Y %H:%M:%S')
        try:
            log['timestamp'] = strtodatetime(log['timestamp'])
        except:
            raise ValueError('Timestamp format: %d-%m-%Y %H:%M:%S')

    if 'http_method' not in log:
        raise ValueError('Missing http method.')
    else:
        if not isinstance(log['http_method'], str):
            raise ValuerError('HTTP method must be a string.')
        if not is_valid_http_method(log['http_method']):
            raise ValueError('Invalid http method. Must be one of: '+str(__HTTP_METHODS__))
    
    if 'resource' not in log:
        raise ValueError('Missing resource.')
    else:
        if not isinstance(log['resource'], str):
            raise ValuerError('Resource must be a string.')

    if 'response_status' not in log:
        raise ValueError('Missing response status.')
    else:
        if not isinstance(log['response_status'], int):
            raise ValuerError('Response status must be an integer')
        try:
            int(log['response_status']) > 0
        except:
            raise ValueError('Reponse status must be a positive integer.')

    if 'size' not in log:
        raise ValueError('Missing size.')
    else:
        if not isinstance(log['size'], int):
            raise ValuerError('Size must be an integer.')
        try:
            int(log['size']) >= 0
        except:
            raise ValueError('Size must be >= 0.')

    if 'referer' not in log:
        expected_args -= 1
    elif not isinstance(log['referer'], str):
        raise ValueError('Referer must be a string.')

    if 'user_agent_string' not in log:
        raise ValueError('Missing user agent string.')
    elif not isinstance(log['user_agent_string'], str):
        raise ValueError('User agent string must be a string.')

    if len(log) != expected_args:
        raise ValueError('Wrong arguments. Was expecting: source_ip, user_id(opt), timestamp(%d-%m-%Y %H:%M:%S), http_method, resource, response_status, size, referer(opt), user_agent_string and type')



def validate_dataxceiver(log):
    expected_args = 6

    if 'timestamp' not in log:
        raise ValueError('Missing timestamp.')
    else:
        if not isinstance(log['timestamp'], str):
            raise ValuerError('Timestamp must be a string with format: %d-%m-%Y %H:%M:%S')
        try:
            log['timestamp'] = strtodatetime(log['timestamp'])
        except:
            raise ValueError('Timestamp format: %d-%m-%Y %H:%M:%S')

    if 'block_id' not in log:
        raise ValueError('Missing block id')
    else:
        if not isinstance(log['block_id'], str):
            raise ValueError('Block id must be a string.')
        if not log['block_id'].startswith('blk_'):
            raise ValueError('Block ids start with "blk_"')
        #Restructure log to fit in database for optimal querying.
        block_id = log['block_id']
        del log['block_id']
        log['block_ids'] = [block_id]

    if 'source_ip' not in log:
        raise ValueError('Missing source ip.')
    else:
        if not isinstance(log['source_ip'], str):
            raise ValueError('Source ip must be a string.')
        if not is_valid_ip(log['source_ip']):
            raise ValueError('Wrong source ip. Must be: [0-255].[0-255].[0-255].[0-255]')

    if 'dest_ip' not in log:
        raise ValueError('Missing destination ip.')
    else:
        if not isinstance(log['dest_ip'], str):
            raise ValueError('Destination ip must be a string.')
        if not is_valid_ip(log['dest_ip']):
            raise ValueError('Wrong destination ip. Must be: [0-255].[0-255].[0-255].[0-255]')
        #Restructure log to fit in database for optimal querying.
        dest_ip = log['dest_ip']
        del log['dest_ip']
        log['dest_ips'] = [dest_ip]

    if 'size' not in log:
        expected_args -= 1
    else:
        if not isinstance(log['size'], int):
            raise ValueError('Size must be an integer.')
        try:
            int(log['size']) > 0
        except:
            raise ValueError('Size must be a positive integer.')

    if len(log) != expected_args:
        raise ValueError('Wrong arguments. Was expecting: timestamp, block_id, source_ip, dest_ip, size(opt) and type')


def validate_namesystem(log):
    if log['type'] == 'replicate':
        expected_args = 5
    if log['type'] == 'delete':
        expected_args = 4

    if 'timestamp' not in log:
        raise ValueError('Missing timestamp.')
    else:
        if not isinstance(log['timestamp'], str):
            raise ValuerError('Timestamp must be a string with format: %d-%m-%Y %H:%M:%S')
        try:
            log['timestamp'] = strtodatetime(log['timestamp'])
        except:
            raise ValueError('Timestamp format: %d-%m-%Y %H:%M:%S')

    if 'block_ids' not in log:
        raise ValueError('Missing block ids.')
    else:
        if not isinstance(log['block_ids'], list):
            raise ValueError('Block ids must be an array.')
        for block in log['block_ids']:
            if not isinstance(block, str):
                raise ValueError('The block ids must be strings. Instead found: '+str(type(block)))
            if not block.startswith('blk_'):
                raise ValueError('Block ids start with "blk_"')


    if 'source_ip' not in log:
        raise ValueError('Missing source ip.')
    else:
        if not isinstance(log['source_ip'], str):
            raise ValueError('Source ip must be a string.')
        if not is_valid_ip(log['source_ip']):
            raise ValueError('Wrong source ip. Must be: [0-255].[0-255].[0-255].[0-255]')

    if log['type'] == 'replicate':

        if 'dest_ips' not in log:
            raise ValueError('Missing destination ips.')
        else:
            if not isinstance(log['dest_ips'], list):
                raise ValueErrror('Destination ips must be an array')
            for ip in log['dest_ips']:
                if not isinstance(ip, str):
                    raise ValueError('The destination ips must be strings. Instead found: '+str(type(ip)))
                if not is_valid_ip(ip):
                    raise ValueError('Wrong destination ip ('+ip+'). Must be: [0-255].[0-255].[0-255].[0-255]')

    if len(log) != expected_args:
        raise ValueError('Wrong arguments: Was expecting timestamp, block_ids, source_ip, dest_ips(only for replicate) and type')
