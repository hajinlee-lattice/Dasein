import pytest
from latticeengines.cf.kafka.profile import KafkaProfile


def test_neg_brokers():
    with pytest.raises(Exception):
        KafkaProfile({
            "BrokerInstanceType": "t2.micro",
            "Brokers": 0
        })

def test_invalid_type():
    with pytest.raises(Exception):
        KafkaProfile({
            "BrokerInstanceType": "nope"
        })

def test_insufficient_computing_resource():
    with pytest.raises(Exception):
        KafkaProfile({
            "BrokerInstanceType": "t2.medium",
            "Instances": 2,
            "Brokers": 2,
            "BrokerMemory": 1000
        })