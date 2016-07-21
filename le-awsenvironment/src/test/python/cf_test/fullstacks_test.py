from latticeengines.cf import zookeeper
from latticeengines.cf.kafka import manage as kafka

TEST_ENV = "dev"


def test_kafka():
    kafka.template(TEST_ENV)

def test_zookeeepr():
    zookeeper.template(TEST_ENV, 3, upload=False)