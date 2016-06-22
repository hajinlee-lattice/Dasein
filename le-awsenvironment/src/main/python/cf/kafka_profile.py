import json
import math
import os

type_def = {
    "t2.micro": {"core": 1, "mem_gb": 1},
    "t2.small": {"core": 1, "mem_gb": 2},
    "t2.medium": {"core": 2, "mem_gb": 4},
    "t2.large": {"core": 2, "mem_gb": 8},
    "m3.medium": {"core": 1, "mem_gb": 3.75},
    "m3.large": {"core": 2, "mem_gb": 7.5},
    "m3.xlarge": {"core": 4, "mem_gb": 15},
    "m3.2xlarge": {"core": 8, "mem_gb": 30},
    "m4.large": {"core": 2, "mem_gb": 8},
    "m4.xlarge": {"core": 4, "mem_gb": 16},
    "m4.2xlarge": {"core": 8, "mem_gb": 32},
    "m4.4xlarge": {"core": 16, "mem_gb": 64},
    "m4.10xlarge": {"core": 40, "mem_gb": 160},
    "c4.large": {"core": 2, "mem_gb": 3.75},
    "c4.xlarge": {"core": 4, "mem_gb": 7.5},
    "c4.2xlarge": {"core": 8, "mem_gb": 15},
    "c4.4xlarge": {"core": 16, "mem_gb": 30},
    "c4.8xlarge": {"core": 36, "mem_gb": 60}
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

        if "InstanceType" in profile:
            self._set_instance_type(profile["InstanceType"])
        else:
            self._instance_type = 't2.medium'

        # ec2 instances
        if "Instances" in profile:
            self._set_instances(profile["Instances"])
        else:
            self._set_instances(3)

        # number of brokers
        if "Brokers" in profile:
            self._set_brokers(profile["Brokers"])
        else:
            self._set_brokers(self._instances)

        # MB
        self._broker_mem = self.__read_from_profile("BrokerMemory", 1024)

    def __read_from_profile(self, key, default):
        return self._profile[key] if key in self._profile else default

    def _set_instances(self, instances):
        assert isinstance(instances, int)
        self._instances = instances

    def _set_instance_type(self, instance_type):
        assert instance_type in type_def
        self._instance_type = instance_type

    def _set_brokers(self, brokers):
        assert isinstance(brokers, int)
        assert brokers >= 1

        # reserved_instances = 2 if self._instance_type == 't2.micro' else 1
        reserved_instances = 0
        if brokers > self._instances - reserved_instances:
            self._primary_bkrs = self._instances - reserved_instances
            self._secondary_bkrs = brokers - self._primary_bkrs
        else:
            self._primary_bkrs = brokers
            self._secondary_bkrs = 0

    def _set_broker_mem(self, mem):
        self._broker_mem = mem

    def _instance_capacity(self):
        instance_mem = (type_def[self._instance_type]["mem_gb"]) * 1024
        instance_capacity = min(
            math.floor((instance_mem - 512) / self._broker_mem)
        )
        if instance_capacity > 2:
            raise ValueError("The instance type you required is too big " +
                             "for the cpu and memory you allocated to brokers. " +
                             "The instance has %d mb mem," % instance_mem +
                             "but you only allocated %d mem to your broker. " % self._broker_mem +
                             "You can either choose a smaller instance, or allocate more resource to broker.")

        instance_capacity = min(2, instance_capacity)
        if instance_capacity < 1:
            raise ValueError("The instance type you required is too small " +
                             "for the cpu and memory you allocated to brokers." +
                             "The instance only has %d mb mem," % instance_mem +
                             "but you allocated %d mem to your broker" % self._broker_mem +
                             "You can either choose a bigger instance, or allocate less resource to broker.")

        return instance_capacity

    def _max_bkrs(self):
        instance_capacity = self._instance_capacity()
        reserved_instances = 5 if self._instance_type == 't2.micro' else 3
        max_bkrs = instance_capacity * (self._instances - reserved_instances)
        if max_bkrs < 1:
            raise ValueError("After excluding %d reserved nodes for zookeeper, schema registry and kafka rest, " % reserved_instances +
                             "you cannot provision any more brokers.")
        return max_bkrs

    def _tot_bkrs(self):
        return self._primary_bkrs + self._secondary_bkrs

    def _validate(self):
        if self._broker_cpu < 512:
            raise ValueError("Need at least 512 cpu unit for each broker")

        if self._broker_mem < 1024:
            raise ValueError("Need at least 1024 MB memory for each broker")

        # sufficient instances
        max_bkrs = self._max_bkrs()
        if self._tot_bkrs() > max_bkrs:
            instance_mem = (type_def[self._instance_type]["mem_gb"]) * 1024
            instance_capacity = self._instance_capacity()
            raise ValueError(
                "For the chosen instance type %s (mem=%d) and required mem=%d, you can support at most %d brokers in total, and %d on each" % \
                (self._instance_type, instance_mem, self._broker_mem, max_bkrs, instance_capacity) )

    def num_instances(self):
        return str(self._instances)

    def instance_type(self):
        return self._instance_type

    def max_instances(self):
        return str(max(2 * self._instances, 3))

    def num_pub_bkrs(self):
        return str(self._primary_bkrs)

    def num_pri_bkrs(self):
        return str(self._secondary_bkrs)

    def bkr_mem(self):
        return str(self._broker_mem)


PROFILE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'profile', 'kafka')
DEFAULT_PROFILE = KafkaProfile(os.path.join(PROFILE_DIR, "default.json"))
