from optparse import OptionParser

import fastavro as avro


class AvroReader(object):
    
    def __init__(self):
        pass
    
    def getData(self, options):
        with open(options["infile"]) as fd:
            reader = avro.reader(fd)
            
            i = 0
            maxRows = int(options["numrows"])
            columns = options["columns"].split(",")
            for row in reader:
                y = [row[column] for column in columns]
                print y
                i = i+1
                if i >= maxRows:
                    break
    
if __name__ == "__main__":
    ar = AvroReader()
    parser = OptionParser()
    parser.add_option("-i", "--infile", dest="infile")
    parser.add_option("-n", "--numrows", dest="numrows")
    parser.add_option("-c", "--columns", dest="columns")
    (options, _) = parser.parse_args()
    ar.getData(options.__dict__)
    