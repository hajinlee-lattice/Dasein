package com.latticeengines.aws.ecs;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SpawnECSTaskRequest {
    @JsonProperty("ClusterName")
    private String clusterName;

    @JsonProperty("TaskDefName")
    private String taskDefName;

    @JsonProperty("ContainerName")
    private String containerName;

    @JsonProperty("CmdLine")
    private String cmdLine;

    @JsonProperty("TaskSubnets")
    private String taskSubnets;

    @JsonProperty("LaunchType")
    private String launchType;

    @JsonProperty("SecurityGroups")
    private String securityGroups;

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getTaskDefName() {
        return taskDefName;
    }

    public void setTaskDefName(String taskDefName) {
        this.taskDefName = taskDefName;
    }

    public String getContainerName() {
        return containerName;
    }

    public void setContainerName(String containerName) {
        this.containerName = containerName;
    }

    public String getCmdLine() {
        return cmdLine;
    }

    public void setCmdLine(String cmdLine) {
        this.cmdLine = cmdLine;
    }

    public String getTaskSubnets() {
        return taskSubnets;
    }

    public void setTaskSubnets(String taskSubnets) {
        this.taskSubnets = taskSubnets;
    }

    public String getLaunchType() {
        return launchType;
    }

    public void setLaunchType(String launchType) {
        this.launchType = launchType;
    }

    public String getSecurityGroups() {
        return securityGroups;
    }

    public void setSecurityGroups(String securityGroups) {
        this.securityGroups = securityGroups;
    }

    @Override
    public String toString() {
        return String.format(
                "clusterName: %s, taskDefName: %s, containerName: %s, cmdLine: %s, taskSubnets: %s, "
                        + "launchType: %s, securityGroups: %s",
                clusterName, taskDefName, containerName, cmdLine, taskSubnets, launchType, securityGroups);
    }
}
