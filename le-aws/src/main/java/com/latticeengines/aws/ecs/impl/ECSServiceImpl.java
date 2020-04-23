package com.latticeengines.aws.ecs.impl;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.amazonaws.services.ecr.AmazonECR;
import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.ecs.model.AssignPublicIp;
import com.amazonaws.services.ecs.model.AwsVpcConfiguration;
import com.amazonaws.services.ecs.model.ContainerOverride;
import com.amazonaws.services.ecs.model.CreateClusterRequest;
import com.amazonaws.services.ecs.model.CreateClusterResult;
import com.amazonaws.services.ecs.model.DescribeTasksRequest;
import com.amazonaws.services.ecs.model.DescribeTasksResult;
import com.amazonaws.services.ecs.model.Failure;
import com.amazonaws.services.ecs.model.LaunchType;
import com.amazonaws.services.ecs.model.ListTaskDefinitionsRequest;
import com.amazonaws.services.ecs.model.ListTaskDefinitionsResult;
import com.amazonaws.services.ecs.model.NetworkConfiguration;
import com.amazonaws.services.ecs.model.RunTaskRequest;
import com.amazonaws.services.ecs.model.RunTaskResult;
import com.amazonaws.services.ecs.model.Task;
import com.amazonaws.services.ecs.model.TaskOverride;
import com.latticeengines.aws.ecs.ECSService;
import com.latticeengines.aws.ecs.SpawnECSTaskRequest;

@Component("ecsService")
public class ECSServiceImpl implements ECSService {
    private static final Logger log = LoggerFactory.getLogger(ECSServiceImpl.class);

    @Inject
    private AmazonECS ecsClient;

    @Inject
    private AmazonECR ecrClient;

    @Override
    public String spawnECSTask(SpawnECSTaskRequest request) throws Exception {
        log.info("Request parameters are " + request.toString());

        CreateClusterResult ret0 = ecsClient
                .createCluster(new CreateClusterRequest().withClusterName(request.getClusterName()));
        log.info(ret0.getCluster().getClusterName() + ", " + ret0.getCluster().getClusterArn());

        ListTaskDefinitionsResult ret1 = ecsClient
                .listTaskDefinitions(new ListTaskDefinitionsRequest().withFamilyPrefix(request.getTaskDefName()));
        if (ret1.getTaskDefinitionArns().size() <= 0) {
            log.error("task definition " + request.getTaskDefName() + " not found");
            throw new Exception("task definition " + request.getTaskDefName() + " not found");
        } else
            log.info(request.getTaskDefName() + ", " + ret1.getTaskDefinitionArns().get(0));

        RunTaskResult ret2 = ecsClient.runTask(new RunTaskRequest().withCluster(request.getClusterName())
                .withTaskDefinition(request.getTaskDefName()).withCount(1)
                .withLaunchType(LaunchType.fromValue(request.getLaunchType()))
                // .withStartedBy(workerId)
                .withOverrides(new TaskOverride().withContainerOverrides(
                        new ContainerOverride().withName(request.getContainerName()).withCommand(request.getCmdLine())))
                .withNetworkConfiguration(new NetworkConfiguration().withAwsvpcConfiguration(new AwsVpcConfiguration()
                        .withSubnets(request.getTaskSubnets().split(",")).withAssignPublicIp(AssignPublicIp.DISABLED)
                        .withSecurityGroups(request.getSecurityGroups().split(",")))));

        List<Failure> failures = ret2.getFailures();
        if (failures.size() != 0) {
            log.error(failures.get(0).getArn() + ", " + failures.get(0).getReason());
            throw new Exception("request to spawn ECS task failed");
        }
        log.info(ret2.getTasks().get(0).getTaskArn() + ", " + ret2.getTasks().get(0).getLastStatus());

        return ret2.getTasks().get(0).getTaskArn();
    }

    @Override
    public List<Task> getTasks(String clusterName, List<String> taskArns) {
        DescribeTasksResult ret0 = ecsClient
                .describeTasks(new DescribeTasksRequest().withCluster(clusterName).withTasks(taskArns));
        return ret0.getTasks();
    }
}
