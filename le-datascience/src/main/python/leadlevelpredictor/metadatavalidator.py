import json
import sys

class MetadataValidator(object):
    
    def __init__(self, fileName):
        self.fileName = fileName
    
    def validate(self):
        metadata = json.loads(open(self.fileName, "r").read())
        for m in metadata["Metadata"]:
            if "Tags" in m and m["Tags"] is not None and "Internal" in m["Tags"]:
                print("%s, %s, %s, %s" % (m["ColumnName"], m["ApprovedUsage"][0], m["Extensions"], m["Tags"]))


if __name__ == "__main__":
    mv = MetadataValidator(sys.argv[1])
    mv.validate()
