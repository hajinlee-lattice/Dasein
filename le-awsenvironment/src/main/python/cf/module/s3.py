from .iam import User
from .resource import Resource


class BucketPolicy(Resource):
    def __init__(self, logicalId, user):
        assert isinstance(user, User)
        Resource.__init__(self, logicalId)
        self._template = {
            "Type" : "AWS::S3::BucketPolicy"
        }

class ReadBucketPolicy(BucketPolicy):
    def __init__(self, logicalId, user, bucket):
        BucketPolicy.__init__(self, logicalId, user)
        self._template = {
            "Type" : "AWS::S3::BucketPolicy",
            "Properties" : {
                "PolicyDocument": {
                    "Version"      : "2008-10-17",
                    "Id"           : logicalId,
                    "Statement"    : [{
                        "Sid"        : "ReadAccess",
                        "Action"     : ["s3:GetObject"],
                        "Effect"     : "Allow",
                        "Resource"   : "arn:aws:s3:::%s/*" % bucket,
                        "Principal"  : { "AWS": {"Fn::GetAtt" : [user.logical_id(), "Arn"]} }
                    }]
                },
                "Bucket" : bucket
            }
        }