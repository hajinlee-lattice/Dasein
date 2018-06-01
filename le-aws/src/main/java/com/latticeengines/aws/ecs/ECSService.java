package com.latticeengines.aws.ecs;

import java.util.List;

import com.amazonaws.services.ecs.model.KeyValuePair;
import com.amazonaws.services.ecs.model.LogConfiguration;
import com.amazonaws.services.ecs.model.Task;

public interface ECSService {
    String spawECSTask(String clusterName, String containerName, String taskDefName,
                       String dockerImageName, String cmdLine, String workerId,
                       String taskRole, String execRole,
                       int cpu, int memory,
                       String taskSubnets, LogConfiguration logConf,
                       KeyValuePair... envVars) throws Exception;

    List<Task> getTasks(String clusterName, List<String> taskArns);
}
