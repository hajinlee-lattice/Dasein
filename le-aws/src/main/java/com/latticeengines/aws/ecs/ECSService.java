package com.latticeengines.aws.ecs;

import java.util.List;

import com.amazonaws.services.ecs.model.Task;

public interface ECSService {
    /*String spawECSTask(String clusterName, String containerName, String taskDefName,
                       String dockerImageName, String cmdLine, String workerId,
                       String taskRole, String execRole,
                       int cpu, int memory,
                       String taskSubnets, LogConfiguration logConf,
                       KeyValuePair... envVars) throws Exception;*/

    String spawECSTask(String clusterName, String taskDefName, String containerName,
                       String cmdLine, String taskSubnets) throws Exception;

    List<Task> getTasks(String clusterName, List<String> taskArns);
}
