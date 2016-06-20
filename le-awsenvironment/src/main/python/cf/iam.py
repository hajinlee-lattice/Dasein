from .resource import Resource
from .template import Template

class Role(Resource):
    def __init__(self, logicalId):
        Resource.__init__(self, logicalId)

        self._template = {
            "Type": "AWS::IAM::Role",
            "Properties": {
                "AssumeRolePolicyDocument": {
                    "Version" : "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {
                                "Service": [
                                    "ec2.amazonaws.com"
                                ]
                            },
                            "Action": [
                                "sts:AssumeRole"
                            ]
                        }
                    ]
                },
                "Path": "/",
                "Policies": [ ]
            }
        }

    def add_statement(self, statement):
        self._template["Properties"]["AssumeRolePolicyDocument"]["Statement"].append(statement)

    def add_policy(self, policy):
        assert isinstance(policy, RolePolicy)
        self._template["Properties"]["Policies"].append(policy.template())



class ECSServiceRole(Role):
    def __init__(self, logicalId):
        Role.__init__(self, logicalId)
        allowed_actions = [
            "ecs:*",
            "ecr:*",
            "logs:CreateLogStream",
            "logs:PutLogEvents",
            "elasticloadbalancing:Describe*",
            "elasticloadbalancing:DeregisterInstancesFromLoadBalancer",
            "elasticloadbalancing:RegisterInstancesWithLoadBalancer",
            "ec2:Describe*",
            "ec2:AuthorizeSecurityGroupIngress"
        ]
        self.add_policy(RolePolicy("ecs-service", allowed_actions))
        self.add_statement({
            "Effect": "Allow",
            "Principal": {
                "Service": [
                    "ecs.amazonaws.com"
                ]
            },
            "Action": [
                "sts:AssumeRole"
            ]
        })

class RolePolicy(Template):
    def __init__(self, name, allowed_actions):
        Template.__init__(self)
        self._template =  {
            "PolicyName": name,
            "PolicyDocument": {
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": allowed_actions,
                        "Resource": "*"
                    }
                ]
            }
        }

class InstanceProfile(Resource):
    def __init__(self, logicalId, role):
        assert isinstance(role, Role)

        Resource.__init__(self, logicalId)
        self._template =  {
            "Type": "AWS::IAM::InstanceProfile",
            "Properties": {
                "Path": "/",
                "Roles": [ role.ref() ]
            }
        }