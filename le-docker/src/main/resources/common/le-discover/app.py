from flask import Flask, request

app = Flask(__name__)
server_map = {}

@app.route("/advertiseip")
def get_advertise_ip():
    return request.remote_addr

if __name__ == "__main__":
    app.run(host='0.0.0.0')