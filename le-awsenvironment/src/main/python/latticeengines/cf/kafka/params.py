from ..module.parameter import Parameter, InstanceTypeParameter, ArnParameter

PARAM_BROKER_INSTANCE_TYPE = InstanceTypeParameter("BrokerInstanceType", "EC2 instance type for Kafka brokers", default="r3.large")
PARAM_SR_INSTANCE_TYPE = InstanceTypeParameter("SRInstanceType", "EC2 instance type for Schema Registry and Kafka Connect")
PARAM_DOCKER_IMAGE_TAG = Parameter("ImageTag", "The tag of docker image to use", default="latest")
PARAM_BROKER_GROUP_CAPACITY = Parameter("DesiredCapacity", "Initial number of EC2 instances for Kafka brokers", type="Number", default="4")
PARAM_BROKER_GROUP_MAX_SIZE = Parameter("MaxSize", "Maximum number of Kafka brokers", type="Number", default="10")
PARAM_ZK_HOSTS = Parameter("ZookeeperHosts", "Zookeeper cluster connection string")
PARAM_BROKERS = Parameter("Brokers", "Desired number of Kafka broker tasks", type="Number", default="4")
PARAM_BROKER_MEMORY = Parameter("BrokerMemory", "Desired number of Kafka broker tasks", type="Number", default="4")
PARAM_BROKER_HEAP_SIZE = Parameter("BrokerHeapSize", "Desired number of Kafka broker tasks", default="3072m")
PARAM_ECS_INSTANCE_PROFILE = ArnParameter("EcsInstanceProfile", "InstanceProfile for ECS instances auto scaling group")
PARAM_EFS_SECURITY_GROUP = Parameter("EfsSecurityGroup", "Security Group for EFS", type="AWS::EC2::SecurityGroup::Id")

KAFKA_PARAMS = [
    PARAM_BROKER_INSTANCE_TYPE,
    PARAM_SR_INSTANCE_TYPE,
    PARAM_DOCKER_IMAGE_TAG,
    PARAM_BROKER_GROUP_CAPACITY,
    PARAM_BROKER_GROUP_MAX_SIZE,
    PARAM_ZK_HOSTS,
    PARAM_BROKERS,
    PARAM_BROKER_MEMORY,
    PARAM_BROKER_HEAP_SIZE,
    PARAM_ECS_INSTANCE_PROFILE,
    PARAM_EFS_SECURITY_GROUP
]