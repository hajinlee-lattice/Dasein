import json
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

        self._instances = self.__read_from_profile("Instances", 3)
        self._instance_type = self.__read_from_profile("InstanceType", "t2.medium")

        self._brokers = self.__read_from_profile("Brokers", 4)
        self._broker_mem = self.__read_from_profile("BrokerMemory", 2048)

        self._registries = self.__read_from_profile("SchemaRegistries", 2)
        self._registry_mem = self.__read_from_profile("SchemaRegistryMemory", 1024)

        self._workers = self.__read_from_profile("ConnectWorkers", 2)
        self._worker_mem = self.__read_from_profile("ConnectWorkerMemory", 1024)

    def __read_from_profile(self, key, default):
        return self._profile[key] if key in self._profile else default

    def _validate(self):
        if min(self._broker_mem, self._worker_mem, self._registry_mem) < 1024:
            raise ValueError("Need at least 1024 MB memory for each container")

        tot_mem_avail = ( type_def[self._instance_type]["mem_gb"] * 1024 - 256 ) * self._instances
        tot_mem_needed = self._brokers * self._broker_mem + self._registries + self._registry_mem + self._workers * self._worker_mem
        if tot_mem_avail < tot_mem_needed:
            raise ValueError("You request %d mb memory in total, but only %d mb can be provided by then instances in your profile.")

    def num_instances(self):
        return str(self._instances)

    def instance_type(self):
        return self._instance_type

    def max_instances(self):
        return str(max(2 * self._instances, 3))

    def num_brokers(self):
        return str(self._brokers)

    def broker_mem(self):
        return str(self._broker_mem)

    def num_registries(self):
        return str(self._brokers)

    def registry_mem(self):
        return str(self._broker_mem)

    def num_workers(self):
        return str(self._brokers)

    def worker_mem(self):
        return str(self._worker_mem)

PROFILE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'profile', 'kafka')
DEFAULT_PROFILE = KafkaProfile(os.path.join(PROFILE_DIR, "default.json"))
