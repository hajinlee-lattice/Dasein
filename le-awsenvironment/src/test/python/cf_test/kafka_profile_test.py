import pytest
from cf.kafka_profile import KafkaProfile


def test_neg_brokers():
    with pytest.raises(Exception):
        KafkaProfile({
            "InstanceType": "t2.micro",
            "Brokers": 0
        })

def test_insufficient_instances():
    with pytest.raises(Exception):
        KafkaProfile({
            "InstanceType": "t2.micro",
            "Instances": 2,
            "Brokers": 2
        })

def test_invalid_type():
    with pytest.raises(Exception):
        KafkaProfile({
            "InstanceType": "nope"
        })

def test_insufficient_computing_resource():
    with pytest.raises(Exception):
        KafkaProfile({
            "InstanceType": "t2.medium",
            "Instances": 2,
            "Brokers": 2,
            "BrokerCPU": 500
        })

    with pytest.raises(Exception):
        KafkaProfile({
            "InstanceType": "t2.medium",
            "Instances": 2,
            "Brokers": 2,
            "BrokerMemory": 1000
        })