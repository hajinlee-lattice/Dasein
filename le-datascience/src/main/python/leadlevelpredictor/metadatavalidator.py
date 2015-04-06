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
            if m["ApprovedUsage"] is None or "ApprovedUsage" not in m:
                print("Null approved usage for %s" % m["ColumnName"])
            if m["ApprovedUsage"] is not None and len(m["ApprovedUsage"]) > 1:
                print("%d approved usages for %s with values %s" % (len(m["ApprovedUsage"]), m["ColumnName"], m["ApprovedUsage"]))
            if "Tags" in m and m["ApprovedUsage"] is not None and m["ApprovedUsage"][0] == "ModelAndAllInsights" and (m["Tags"] is None or "Tags" not in m):
                print("Null tags for %s, %s" % (m["ColumnName"], m["ApprovedUsage"][0]))


if __name__ == "__main__":
    mv = MetadataValidator(sys.argv[1])
    mv.validate()
