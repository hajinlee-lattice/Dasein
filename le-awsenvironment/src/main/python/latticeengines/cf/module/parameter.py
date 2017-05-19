
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

    def name(self):
        return self._name

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

class EnvVarParameter(Parameter):
    def __init__(self, name, type="String", default="", allowed_values=None):
        Parameter.__init__(self, name.replace("_", ''), "Environment Variable: " + name, type=type, default=default, allowed_values=allowed_values)

class InstanceTypeParameter(Parameter):
    ALLOWED_INSTANCE_TYPES = (
        "t2.micro",
        "t2.small",
        "t2.medium",
        "m4.large",
        "m4.xlarge",
        "c4.large",
        "c4.xlarge",
        "c4.2xlarge",
        "r3.large",
        "r3.xlarge",
        "r4.large",
        "r4.xlarge"
    )
    def __init__(self, name, description, default="t2.medium"):
        assert default in InstanceTypeParameter.ALLOWED_INSTANCE_TYPES
        Parameter.__init__(self, name, description, type="String", default=default, allowed_values=list(InstanceTypeParameter.ALLOWED_INSTANCE_TYPES))


PARAM_VPC_ID = Parameter("VpcId", "The VPC in which the stack will reside", type="AWS::EC2::VPC::Id")
PARAM_SUBNET_1 = Parameter("SubnetId1", "The first subnet to provision EC2 instances.", type="AWS::EC2::Subnet::Id")
PARAM_SUBNET_2 = Parameter("SubnetId2", "The second subnet to provision EC2 instances.", type="AWS::EC2::Subnet::Id")
PARAM_SUBNET_3 = Parameter("SubnetId3", "The third subnet to provision EC2 instances.", type="AWS::EC2::Subnet::Id")
PARAM_KEY_NAME = Parameter("KeyName", "Name of an existing EC2 KeyPair to enable SSH access to the instance", type="AWS::EC2::KeyPair::KeyName")
PARAM_ENVIRONMENT = Parameter("Environment", "Environment tag. For example, qacluster and prodcluster")

PARAM_SECURITY_GROUP = Parameter("SecurityGroupId", "The security group to be used by EC2", type="AWS::EC2::SecurityGroup::Id")
PARAM_INSTANCE_TYPE = InstanceTypeParameter("InstanceType", "EC2 instance type")

PARAM_ECS_INSTANCE_PROFILE_ARN = ArnParameter("EcsInstanceProfile", "InstanceProfile for ECS instances auto scaling group")
PARAM_ECS_INSTANCE_PROFILE_NAME = Parameter("EcsInstanceProfileName", "InstanceProfile NAME for ECS instances auto scaling group")
PARAM_ECS_INSTANCE_ROLE_NAME = Parameter("EcsInstanceRoleName", "EC2 Role Name for ECS instances auto scaling group")
PARAM_ELB_NAME = Parameter("ElbName", "Name of the main elastic load balancer name")
PARAM_TARGET_GROUP=ArnParameter("TargetGroupArn", "Arn of the target group for load balancer")
PARAM_CAPACITY = Parameter("DesiredCapacity", "Desired number of containers", type="Number", default="2")
PARAM_MAX_CAPACITY = Parameter("MaximumCapacity", "Desired number of containers", type="Number", default="8")

PARAM_SSL_CERTIFICATE_ARN = ArnParameter("LatticeCertificateArn", "Arn of lattice wildcard certificate.")

COMMON_PARAMETERS = {
    PARAM_VPC_ID,
    PARAM_SUBNET_1,
    PARAM_SUBNET_2,
    PARAM_SUBNET_3,
    PARAM_KEY_NAME,
    PARAM_ENVIRONMENT
}

ECS_PARAMETERS = {
    PARAM_SECURITY_GROUP,
    PARAM_INSTANCE_TYPE,
    PARAM_ECS_INSTANCE_PROFILE_ARN,
    PARAM_ECS_INSTANCE_PROFILE_NAME,
    PARAM_ECS_INSTANCE_ROLE_NAME,
    PARAM_TARGET_GROUP,
    PARAM_CAPACITY,
    PARAM_MAX_CAPACITY
}