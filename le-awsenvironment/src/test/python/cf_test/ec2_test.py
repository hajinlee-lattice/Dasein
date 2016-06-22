from cf.module.ec2 import EC2Instance

def test_template():
    t = EC2Instance("EC2")
    print t.json()
    assert "CreationPolicy" in t.template()