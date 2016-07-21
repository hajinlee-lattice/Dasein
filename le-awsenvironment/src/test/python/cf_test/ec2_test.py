from latticeengines.cf.module.ec2 import EC2Instance
from latticeengines.cf.module.parameter import PARAM_SUBNET_1, PARAM_KEY_NAME

def test_template():
    t = EC2Instance("EC2", "Subnet", PARAM_SUBNET_1, PARAM_KEY_NAME)
    print t.json()
    assert "CreationPolicy" in t.template()