from cf.module.ec2 import EC2Instance
from cf.module.stack import Stack


def test_template():
    t = Stack("Testing stack")
    ec2 = EC2Instance("EC2")
    t.add_ec2(ec2)
    assert t.template()["Description"] == "Testing stack"
    assert "Mappings" in t.template()
    assert "Parameters" in t.template()
    assert "Properties" not in t.template()