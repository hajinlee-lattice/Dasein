package com.latticeengines.aws.ecs.impl;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.amazonaws.services.ecr.AmazonECR;
import com.amazonaws.services.ecr.model.GetAuthorizationTokenRequest;
import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.ecs.model.AssignPublicIp;
import com.amazonaws.services.ecs.model.AwsVpcConfiguration;
import com.amazonaws.services.ecs.model.Compatibility;
import com.amazonaws.services.ecs.model.ContainerDefinition;
import com.amazonaws.services.ecs.model.ContainerOverride;
import com.amazonaws.services.ecs.model.CreateClusterRequest;
import com.amazonaws.services.ecs.model.CreateClusterResult;
import com.amazonaws.services.ecs.model.DescribeTasksRequest;
import com.amazonaws.services.ecs.model.DescribeTasksResult;
import com.amazonaws.services.ecs.model.Failure;
import com.amazonaws.services.ecs.model.KeyValuePair;
import com.amazonaws.services.ecs.model.LaunchType;
import com.amazonaws.services.ecs.model.LogConfiguration;
import com.amazonaws.services.ecs.model.NetworkConfiguration;
import com.amazonaws.services.ecs.model.NetworkMode;
import com.amazonaws.services.ecs.model.RegisterTaskDefinitionRequest;
import com.amazonaws.services.ecs.model.RegisterTaskDefinitionResult;
import com.amazonaws.services.ecs.model.RunTaskRequest;
import com.amazonaws.services.ecs.model.RunTaskResult;
import com.amazonaws.services.ecs.model.Task;
import com.amazonaws.services.ecs.model.TaskOverride;
import com.latticeengines.aws.ecs.ECSService;

@Component("ecsService")
public class ECSServiceImpl implements ECSService {
    private static final Logger log = LoggerFactory.getLogger(ECSServiceImpl.class);

    @Inject
    private AmazonECS ecsClient;
    @Inject
    private AmazonECR ecrClient;

    @Override
    public String spawECSTask(String clusterName, String containerName, String taskDefName,
                              String dockerImageName, String cmdLine, String workerId,
                              String taskRole, String execRole,
                              int cpu, int memory,
                              String taskSubnets, LogConfiguration logConf,
                              KeyValuePair... envVars) throws Exception
    {
        String repoEndpoint = ecrClient.getAuthorizationToken(new GetAuthorizationTokenRequest()).getAuthorizationData().get(0).getProxyEndpoint().replace("https://", "");
        String dockerImageRef = repoEndpoint + "/" + dockerImageName + ":latest";

        CreateClusterResult ret0 = ecsClient.createCluster(new CreateClusterRequest().withClusterName(clusterName));
        log.info(ret0.getCluster().getClusterName() + ", " + ret0.getCluster().getClusterArn());

        RegisterTaskDefinitionResult ret1 = ecsClient.registerTaskDefinition(new RegisterTaskDefinitionRequest()
                .withFamily(taskDefName)
                .withNetworkMode(NetworkMode.Awsvpc)
                .withTaskRoleArn(taskRole)
                .withExecutionRoleArn(execRole)
                .withRequiresCompatibilities(Compatibility.FARGATE)
                .withCpu(Integer.toString(cpu))
                .withMemory(Integer.toString(memory))
                .withContainerDefinitions(new ContainerDefinition()
                        .withName(containerName)
                        .withImage(dockerImageRef)
                        .withCpu(cpu)
                        .withMemory(memory)
                        .withCommand(cmdLine).withLogConfiguration(logConf)));
        log.info(ret1.getTaskDefinition().getFamily() + ", " + ret1.getTaskDefinition().getTaskDefinitionArn());

        RunTaskResult ret2 = ecsClient.runTask(new RunTaskRequest()
                .withCluster(clusterName)
                .withTaskDefinition(taskDefName)
                .withCount(1)
                .withLaunchType(LaunchType.FARGATE)
                .withStartedBy(workerId)
                .withOverrides(new TaskOverride()
                        .withContainerOverrides(new ContainerOverride()
                                .withName(containerName)
                                .withEnvironment(envVars)))
                .withNetworkConfiguration(new NetworkConfiguration()
                        .withAwsvpcConfiguration(new AwsVpcConfiguration()
                                .withSubnets(taskSubnets.split(","))
                                .withAssignPublicIp(AssignPublicIp.DISABLED))));

        List<Failure> failures = ret2.getFailures();
        if (failures.size() != 0) {
            log.error(failures.get(0).getArn() + ", " + failures.get(0).getReason());
            throw new Exception("request to spawn ECS task failed");
        }
        log.info(ret2.getTasks().get(0).getTaskArn() + ", " + ret2.getTasks().get(0).getLastStatus());

        return ret2.getTasks().get(0).getTaskArn();
    }

    @Override
    public List<Task> getTasks(String clusterName, List<String> taskArns)
    {
        DescribeTasksResult ret0 = ecsClient.describeTasks(
                new DescribeTasksRequest()
                        .withCluster(clusterName)
                        .withTasks(taskArns));
        return ret0.getTasks();
    }
}
