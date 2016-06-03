import os
import base64

CONF_DIR = 'conf/env/'
CREDENTIALS_FILE = 'credentials.properties'
PARAMETERS_FILE = 'parameters.properties'


class Environment(object):
    def __init__(self, name):
        self.name = name
        dirname = CONF_DIR + self.name
        if not os.path.isdir(dirname):
            raise Exception("No such environment configured %s" % self.name)

        self.credentials = self.__load_properties(dirname + '/' + CREDENTIALS_FILE)
        self.parameters = self.__load_properties(dirname + '/' + PARAMETERS_FILE)
        self.parameters = [{'ParameterKey': k, 'ParameterValue': v} for k, v in self.parameters]

    def __load_properties(self, filepath):
        with open(filepath, 'r') as file:
            lines = file.readlines()
            out = {}
            for line in lines:
                if not line.isspace():
                    idx = line.find('=')
                    if not idx == -1:
                        key = line[0:idx]
                        val = line[idx+1:]
                        if key.endswith('encrypted'):
                            key = key[0:key.find('.encrypted')]
                            val = base64.b64decode(val.strip())
                        out[key] = val.strip()
            return out
