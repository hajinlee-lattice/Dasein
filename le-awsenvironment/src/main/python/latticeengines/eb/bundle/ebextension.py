

class Extension:
    def __init__(self):
        self._content=""
        self._filename=""
        pass

    def filename(self):
        return self._filename

    def content(self):
        return self._content

class ElbListener(Extension):
    def __init__(self):
        Extension.__init__(self)
        self._content = "option_settings: \n"
        self._filename = "elb-listener.config"

    def listen(self, port, instance_port=None, protocol="HTTP", instance_protocol="HTTP"):
        if instance_port is None:
            instance_port = port
        self._content += \
            "  aws:elb:listener:%d: \n" % port+ \
            "    ListenerProtocol: %s \n" % protocol + \
            "    InstanceProtocol: %s \n" % instance_protocol + \
            "    InstancePort: %s \n" % instance_port
        return self

class ElbIngress(Extension):
    __template = \
        "Resources: \n" + \
        "  port8080SecurityGroupIngress: \n" + \
        "    Type: AWS::EC2::SecurityGroupIngress \n" + \
        "    Properties: \n" + \
        "      GroupId: {\"Fn::GetAtt\" : [\"AWSEBSecurityGroup\", \"GroupId\"]} \n" + \
        "      IpProtocol: tcp \n" + \
        "      ToPort: {{to_port}} \n" + \
        "      FromPort: {{from_port}} \n" + \
        "      SourceSecurityGroupName: { \"Fn::GetAtt\": [\"AWSEBLoadBalancer\", \"SourceSecurityGroup.GroupName\"] }\n"

    def __init__(self, from_port, to_port):
        Extension.__init__(self)
        self._content = ElbIngress.__template\
            .replace("{{from_port}}", "%d" % from_port) \
            .replace("{{to_port}}", "%d" % to_port)
        self._filename = "elb-listener.config"


VPC_SUBNET_TEMPLATE= \
"""
option_settings:
  aws:autoscaling:launchconfiguration:
    EC2KeyName: "{{key_name}}"
    InstanceType: "{{instance_type}}"
    SecurityGroups: "{{security_groups}}"
  aws:ec2:vpc:
    VPCId: "{{vpc_id}}"
    Subnets: "{{subnets}}"
    ELBSubnets: "{{elb_subnets}}"
"""

class VpcSubnet(Extension):
    def __init__(self, key_name, instance_type, vpc_id, security_groups, subnets, elb_subnets=None):
        if elb_subnets is None:
            elb_subnets = subnets
        Extension.__init__(self)
        self._content = VPC_SUBNET_TEMPLATE \
            .replace("{{key_name}}", key_name) \
            .replace("{{instance_type}}", instance_type) \
            .replace("{{vpc_id}}", vpc_id) \
            .replace("{{security_groups}}", ','.join(security_groups)) \
            .replace("{{subnets}}", ','.join(subnets)) \
            .replace("{{elb_subnets}}", ','.join(elb_subnets))
        self._filename = "vpc-subnet.config"