import uuid
from datetime import datetime

def get_current_timestamp():
    current = (datetime.utcnow() - datetime.utcfromtimestamp(0)).total_seconds()
    return int(current)

def get_current_date():
    return datetime.utcfromtimestamp(get_current_timestamp()).strftime('%Y-%m-%dT%H:%M:%S.%fZ')

def resolve_bool(st):
    if type(st) != int and type(st) != str and type(st) != bool:
        raise Exception("Error: invalid boolean equivalent")
    if type(st) == bool:
        return st
    if type(st) == int:
        return bool(st)
    if st == "True" or st == "true" or st == "yes":
        return True
    elif st == "False" or st == "false" or st == "no":
        return False
    else:
        raise Exception("Error: invalid boolean equivalent")
    
def uuid_hex():
    uid = uuid.uuid4().hex[:16]
    return uid