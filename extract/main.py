from flask import Flask, request
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

    collection_name.update_many({'page': int(content["page"])}, {
                                "$set": {'numbers': content["numbers"]}}, upsert=True)


@app.route("/extract")
def extract():

    current_page = request.args.get('page')

    if current_page is None:
        return {'statusOk': True, 'message': 'Cannot find page. Please try to use ?page=', 'stillHaveData': True}

    if not current_page.isdigit:
        return {'statusOk': True, 'message': 'Please try to use an integer value for page', 'stillHaveData': True}

    response = requests.get("".join([API_BASE_URL, str(current_page)]))

    if response.status_code == 200:
        json_content = json.loads(response.content)

        if json_content["numbers"]:
            json_content.update({'page': current_page})
            persist(json_content)
            return {'statusOk': True, 'stillHaveData': True, 'currentPage': current_page}

        else:
            return {'statusOk': True, 'stillHaveData': False, 'lastPage': current_page}

    elif response.status_code == 404:
        return {'statusOk': False, 'stillHaveData': True, 'message': 'Cannot access [' + API_BASE_URL + ']'}

    elif response.status_code == 500:
        return {'statusOk': False, 'stillHaveData': True, 'currentPage': current_page}
