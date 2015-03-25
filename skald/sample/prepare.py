import os
import sys
import urllib2


def log(text):
    print text

def replace_tokens(template, tokens):
    for token, value in tokens.items():
        template = template.replace(token, value)
    return template

def make_request(url, header, data):
    request = urllib2.Request(url, headers=header, data=data)
    response = urllib2.urlopen(request)
    if response.code == 200:
        log("Call to {0} successful".format(url))
    else:
        raise Exception("Received code {0} from request".format(response.code))

def main(args):
    skald_address = "http://localhost:8050"

    tokens = {
        "<<model_name>>": "54819c16-3298-4a1d-accb-646f92ae2c3e-PLSModel",
        "<<contract_id>>": "Lattice_Relaunch",
        "<<tenant_id>>": "Lattice_Relaunch",
        "<<space_id>>": "Production"
        }

    script_dir = os.path.abspath(os.path.dirname(__file__))
    with open(os.path.join(script_dir, "ModelTags.json")) as f:
        tags = f.read()
    with open(os.path.join(script_dir, "ModelCombination.json")) as f:
        combination = f.read()
    with open(os.path.join(script_dir, "request.json")) as f:
        record = f.read()

    make_request("{0}/SetModelTags".format(skald_address),
                 {"Content-Type": "application/json"},
                 replace_tokens(tags, tokens))

    make_request("{0}/SetModelCombination".format(skald_address),
                 {"Content-Type": "application/json"},
                 replace_tokens(combination, tokens))

    make_request("{0}/ScoreRecord".format(skald_address),
                 {"Content-Type": "application/json"},
                 record)


if __name__ == '__main__':
    main(sys.argv)
