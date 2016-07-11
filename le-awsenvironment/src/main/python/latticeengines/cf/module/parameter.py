
class Parameter:
    def __init__(self, name, description, type="String", default=None, allowed_values=None):
        self._name = name
        self._type = type
        self._description = description
        self._default = default
        self._allowed_values = allowed_values

    def definition(self):
        defn = {
            "Description": self._description,
            "Type" : self._type
        }

        if self._default is not None:
            defn["Default"] = self._default

        if self._allowed_values is not None:
            defn["AllowedValues"] = self._allowed_values

        return { self._name: defn }


    def ref(self):
        return { "Ref": self._name }

    def config(self, value):
        return {
            'ParameterKey': self._name,
            'ParameterValue': value
        }


class ArnParameter(Parameter):
    def __init__(self, name, description, type="String", default=None, allowed_values=None):
        Parameter.__init__(self, name, description, type=type, default=default, allowed_values=allowed_values)

    def config(self, value):
        self.validate_arn(value)
        return Parameter.config(self, value)

    def validate_arn(self, value):
        if 'arn:aws' != value[:7]:
            raise ValueError("Must provide an ARN number for this parameter. %s does not seem so." % value)

class InstanceTypeParameter(Parameter):
    ALLOWED_INSTANCE_TYPES = (
        "t2.micro",
        "t2.medium",
        "m3.medium",
        "m3.large",
        "m3.xlarge",
        "m4.large",
        "m4.xlarge",
        "r3.large",
        "r3.xlarge"
    )
    def __init__(self, name, description, default="t2.medium"):
        assert default in InstanceTypeParameter.ALLOWED_INSTANCE_TYPES
        Parameter.__init__(self, name, description, type="String", default=default, allowed_values=list(InstanceTypeParameter.ALLOWED_INSTANCE_TYPES))


PARAM_VPC_ID = Parameter("VpcId", "The VPC in which the stack will reside", type="AWS::EC2::VPC::Id")
PARAM_SUBNET_1 = Parameter("SubnetId1", "The first subnet to provision EC2 instances.", type="AWS::EC2::Subnet::Id")
PARAM_SUBNET_2 = Parameter("SubnetId2", "The second subnet to provision EC2 instances.", type="AWS::EC2::Subnet::Id")
PARAM_SUBNET_3 = Parameter("SubnetId3", "The third subnet to provision EC2 instances.", type="AWS::EC2::Subnet::Id")
PARAM_KEY_NAME = Parameter("KeyName", "Name of an existing EC2 KeyPair to enable SSH access to the instance", type="AWS::EC2::KeyPair::KeyName")
PARAM_ENVIRONMENT = Parameter("Environment", "Environment tag. For example, qa and production")

PARAM_SECURITY_GROUP = Parameter("SecurityGroupId", "The security group to be used by EC2", type="AWS::EC2::SecurityGroup::Id")
PARAM_INSTANCE_TYPE = InstanceTypeParameter("InstanceType", "EC2 instance type")

COMMON_PARAMETERS = {
    PARAM_VPC_ID,
    PARAM_SUBNET_1,
    PARAM_SUBNET_2,
    PARAM_SUBNET_3,
    PARAM_KEY_NAME,
    PARAM_ENVIRONMENT
}