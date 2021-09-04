from flask import Flask
from pymongo import MongoClient
import requests
import pymongo
import json

app = Flask(__name__)


API_BASE_URL = "<my-api-url>"
MONGODB_USER = "victor"
MONGODB_PASSWORD = "<my-password>"
MONGODB_DATABASE = "<my-collection>"
MONGODB_CLUSTER = "<my-cluster-on-atlas>"
MONGODC_PARAMS = "?retryWrites=true&w=majority"
MONGODB_PROTOCOL = "mongodb+srv://"

CONNECTION_STRING = "".join([MONGODB_PROTOCOL, MONGODB_USER, ":",
                            MONGODB_PASSWORD, "@", MONGODB_CLUSTER, MONGODB_DATABASE, MONGODC_PARAMS])


def persist(content):
    client = pymongo.MongoClient(CONNECTION_STRING)
    dbname = client['extract']
    collection_name = dbname["extracts"]
    collection_name.insert_many([content])


def getLastPagePersisted():
    client = pymongo.MongoClient(CONNECTION_STRING)
    dbname = client['extract']
    collection_name = dbname["extracts"]

    # Get Page value from collection, sorted by page desc
    data = collection_name.find_one(sort=[("page", pymongo.DESCENDING)])

    # Check if data isnt NoneType (NoneType means that are no valid data)
    if data:
        return data['page']
    else:
        return 0


@app.route("/extract")
def extract():
    current_page = getLastPagePersisted() + 1
    response = requests.get("".join([API_BASE_URL, str(current_page)]))

    if response.status_code == 200:
        json_content = json.loads(response.content)

        if json_content["numbers"]:
            json_content.update({'page': current_page})
            persist(json_content)
            return {'stillHaveData': True, 'currentPage': current_page}

        else:
            return {'stillHaveData': False, 'lastPage': current_page}

    elif response.status_code == 404:
        return {'message': 'Cannot access [' + API_BASE_URL + ']'}
