from flask import Flask, request
from threading import RLock

app = Flask(__name__)
server_map = {}

rlock_map = {}
master_lock = RLock()

quorum_map = {}

@app.route("/internal_addr")
def expose_internal_addr():
    with open('/etc/internaladdr.txt', 'r') as f:
        return f.read()

@app.route("/advertiseip")
def get_advertise_ip():
    return request.remote_addr

@app.route("/quorums/<quorum_name>")
def register_quorum_api(quorum_name):
    host = request.remote_addr
    quorum_size = int(request.args['n']) if 'n' in request.args else 3
    quorum = register_quorum_internal(quorum_name, host)
    if len(quorum) != quorum_size:
        print "Only %d clients have been registered to the quorum of taget size %d" % (len(quorum), quorum_size)
        return ""
    else:
        return print_quorum(quorum)

@app.route("/quorums/<quorum_name>/myid")
def get_myid_in_quorum(quorum_name):
    host = request.remote_addr
    if quorum_name in quorum_map:
        quorum = quorum_map[quorum_name]
        if host in quorum:
            return "%d" % quorum[host]
    return ""

@app.route("/quorums/<quorum_name>/zkhosts")
def get_zkhosts_in_quorum(quorum_name):
    if quorum_name in quorum_map:
        quorum = quorum_map[quorum_name]
        return print_zkhosts(quorum)
    return ""

def acquire_lock(quorum_name):
    master_lock.acquire()
    try:
        if quorum_name not in rlock_map:
            rlock_map[quorum_name] = Rlock()
        rlock_map[quorum_name].acquire()
    except:
        print "Error while registering a reentry lock for the quorum [%s]" % quorum_name
    finally:
        master_lock.release()

def release_lock(quorum_name):
    master_lock.acquire()
    try:
        if quorum_name in rlock_map:
            rlock_map[quorum_name].release()
    except:
        print "Error while registering a reentry lock for the quorum [%s]" % quorum
    finally:
        master_lock.release()

def register_quorum_internal(quorum_name, host):
    acquire_lock(quorum_name)
    try:
        print "Registering %s to quorum %s ..." % (host, quorum_name)
        if quorum_name not in quorum_map:
            quorum_map[quorum_name] = {}

        if host not in quorum_map[quorum_name]:
            next_id = len(quorum_map[quorum_name]) + 1
            quorum_map[quorum_name][host] = next_id

        return quorum_map[quorum_name]
    except:
        print "Error registering [%s] in quorum [%s]" % (host, quorum_name)
    finally:
        release_lock(quorum_name)

def print_quorum(quorum):
    return '\n'.join("server.%d=%s:2888:3888" % (i, h) for h, i in quorum.items())

def print_zkhosts(quorum):
    return ','.join("%s:2181" % h for h in quorum.keys())

if __name__ == "__main__":
    app.run(host='0.0.0.0')