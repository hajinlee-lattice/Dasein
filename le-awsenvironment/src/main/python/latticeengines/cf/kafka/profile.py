import json
import math
import os

type_def = {
    "t2.micro":   { "core": 1, "mem_gb": 1,     "on_demand": 0.013 },
    "t2.medium":  { "core": 2, "mem_gb": 4,     "on_demand": 0.052, "ecs_mem" : 3956 },
    "m4.large":   { "core": 2, "mem_gb": 8,     "on_demand": 0.120 },
    "m4.xlarge":  { "core": 4, "mem_gb": 16,    "on_demand": 0.239 },
    "m4.2xlarge": { "core": 8, "mem_gb": 32,    "on_demand": 0.479 },
    "m3.medium":  { "core": 1, "mem_gb": 3.75,  "on_demand": 0.067 },
    "m3.large":   { "core": 2, "mem_gb": 7.5,   "on_demand": 0.133 },
    "m3.xlarge":  { "core": 4, "mem_gb": 15,    "on_demand": 0.266 },
    "m3.2xlarge": { "core": 8, "mem_gb": 30,    "on_demand": 0.532 },
    "r3.large":   { "core": 2, "mem_gb": 15.25, "on_demand": 0.166 },
    "r3.xlarge":  { "core": 4, "mem_gb": 30.5,  "on_demand": 0.333 },
    "r3.2xlarge": { "core": 8, "mem_gb": 61,    "on_demand": 0.665 }
}

class KafkaProfile:
    def __init__(self, profile):
        if isinstance(profile, str):
            with open(profile) as f:
                self._construct(json.loads(f.read()))
        else:
            self._construct(profile)
        # self._validate()

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
        if self._broker_mem < 1024:
            raise ValueError("Need at least 1024 MB memory for each container")

        tot_mem_avail = type_def[self._instance_type]["ecs_mem"] if "ecs_mem" in type_def[self._instance_type]["ecs_mem"] \
            else ( type_def[self._instance_type]["mem_gb"] * 1024 - 256 ) * self._instances
        tot_mem_needed = self._brokers * self._broker_mem
        if tot_mem_avail < tot_mem_needed:
            raise ValueError("You request %d mb memory in total, but only %d mb can be provided by then instances in your profile.")

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
