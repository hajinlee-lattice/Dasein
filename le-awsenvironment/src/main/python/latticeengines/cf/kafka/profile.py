import json
import math
import os

class KafkaProfile:
    def __init__(self, profile):
        if isinstance(profile, str):
            with open(profile) as f:
                self._construct(json.loads(f.read()))
        else:
            self._construct(profile)
        self._validate()

    def _construct(self, profile):
        self._profile = profile

        self._instances = self.__read_from_profile("Instances", 3)
        self._instance_type = self.__read_from_profile("BrokerInstanceType", "t2.medium")
        self._sr_instance_type = self.__read_from_profile("SRInstanceType", "t2.medium")

        self._brokers = self.__read_from_profile("Brokers", 4)
        self._set_broker_mem(self.__read_from_profile("BrokerMemory", 2048))

    def __read_from_profile(self, key, default):
        return self._profile[key] if key in self._profile else default

    def _validate(self):
        if self._brokers < 2:
            raise ValueError("Need at least 2 brokers")

        if self._broker_mem < 1024:
            raise ValueError("Need at least 1024 MB memory for each container")

    def _set_broker_mem(self, mem):
        self._broker_heap = "%dm" % math.floor(mem / 1.1)
        self._broker_mem = mem

    def num_instances(self):
        return str(self._instances)

    def instance_type(self):
        return self._instance_type

    def sr_instance_type(self):
        return self._sr_instance_type

    def max_instances(self):
        return str(max(2 * self._instances, 4))

    def num_brokers(self):
        return str(self._brokers)

    def broker_mem(self):
        return str(self._broker_mem)

    def broker_heap(self):
        return str(self._broker_heap)


PROFILE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'profile')
DEFAULT_PROFILE = KafkaProfile(os.path.join(PROFILE_DIR, "default.json"))
