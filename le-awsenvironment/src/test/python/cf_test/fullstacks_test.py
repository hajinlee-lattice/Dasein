from cf import kafka, zookeeper

def test_kafka():
    kafka.template_internal()

def test_zookeeepr():
    zookeeper.template_internal(3)