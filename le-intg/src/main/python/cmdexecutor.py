from flask import Flask
from flask import request
import json
import os
import subprocess
import sys


app = Flask(__name__)

@app.route('/')
def index():
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
    
@app.route('/upload', methods = ['POST'])
def upload():
    f = request.files['file']
    uploadedFilePath = os.path.join(app.config['UPLOAD_FOLDER'], f.filename) 
    f.save(uploadedFilePath)
    return "File uploaded to " + uploadedFilePath
    

if __name__ == '__main__':
    app.config['UPLOAD_FOLDER'] = sys.argv[2]
    app.run(host='0.0.0.0', port=int(sys.argv[1]), debug=False)