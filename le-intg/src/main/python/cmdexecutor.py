from flask import Flask
from flask import request
import json
import subprocess
import sys

app = Flask(__name__)

@app.route('/')
def index(cmd):
    return "This is REST Command Line Executor v1.0"

@app.route('/cmd/<cmd>', methods = ['POST', 'GET'])
def runCommand(cmd):
    try:
        jdata = json.loads(request.data)
        if "commands" not in jdata:
            raise "No commands parameter in JSON request body."
        if type(jdata["commands"]) != list:
            raise "Commands parameter is not a list."
        return subprocess.check_output(jdata["commands"])
    except Exception, e:
        return str(e)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(sys.argv[1]), debug=False)