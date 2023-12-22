import os
import json

def ensure_dir_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)
    return path

def convertDict(myblob):
    data = str(myblob.read())
    data = data.replace('b','').replace('\'','')
    json_data = json.loads(data)
    return json_data