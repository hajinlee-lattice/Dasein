import datetime
import inspect
import json

import boto3


def main():   
    client = boto3.client('dms')
    members = inspect.getmembers(client, predicate=inspect.ismethod)
    skip = ['describe_refresh_schemas_status', 'describe_schemas', 'describe_table_statistics']
    for member in members:
        if member[0].startswith('describe_') and member[0] not in skip:
            export(client, member[0])

def export(client, description):
    print 'Exporting ' + description
    describe_method = getattr(client, description)
    response = describe_method()
    resp_json = json.dumps(response, default=datetime_handler)
    f1 = open('./' + description + '.json', 'w+')
    print >> f1, resp_json 

def datetime_handler(x):
    if isinstance(x, datetime.datetime):
        return x.isoformat()
    raise TypeError("Unknown type")

if __name__ == '__main__':
    main()

