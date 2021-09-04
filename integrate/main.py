import json
import requests

stillHaveData = True

# {"currentPage":2,"stillHaveData":true}

while stillHaveData:
    response = requests.get("http://127.0.0.1:5001/extract")
    json_content = json.loads(response.content)

    stillHaveData = json_content["stillHaveData"]

    if stillHaveData:
        currentPage = json_content["currentPage"]
    else:
        currentPage = json_content["lastPage"]

    print("".join(["Page ", str(currentPage),
          ' was persisted successfuly, but we still have data to persist']))

print(
    "".join(["All data has been persisted. The last page is", str(currentPage)]))
