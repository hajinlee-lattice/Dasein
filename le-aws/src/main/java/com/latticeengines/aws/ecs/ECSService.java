package com.latticeengines.aws.ecs;

import java.util.List;

import com.amazonaws.services.ecs.model.Task;

public interface ECSService {
    String spawnECSTask(SpawnECSTaskRequest request) throws Exception;

    List<Task> getTasks(String clusterName, List<String> taskArns);
}
