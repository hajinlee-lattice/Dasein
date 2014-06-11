package com.latticeengines.domain.exposed.dataplatform.dlorchestration;

import java.util.List;

import com.latticeengines.domain.exposed.dataplatform.HasId;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

public class ModelCommand implements HasPid, HasId<Integer> {

    private Long pid;
    private int commandId;
    private String deploymentExternalId;
    private ModelCommandStatus commandStatus;
    private List<ModelCommandParameter> commandParameters;
    private ModelCommandStep modelCommandStep;

    @Override
    public Long getPid() {
        return this.pid;
    }

    @Override
    public void setPid(Long id) {
        this.pid = id;
    }

    @Override
    public Integer getId() {
        return commandId;
    }

    @Override
    public void setId(Integer id) {
        this.commandId = id;
    }

    public String getDeploymentExternalId() {
        return deploymentExternalId;
    }

    public void setDeploymentExternalId(String deploymentExternalId) {
        this.deploymentExternalId = deploymentExternalId;
    }

    public ModelCommandStatus getCommandStatus() {
        return commandStatus;
    }

    public void setCommandStatus(ModelCommandStatus commandStatus) {
        this.commandStatus = commandStatus;
    }

    public List<ModelCommandParameter> getCommandParameters() {
        return commandParameters;
    }

    public void setCommandParameters(List<ModelCommandParameter> commandParameters) {
        this.commandParameters = commandParameters;
    }

    public boolean isNew() {
        return commandStatus.equals(ModelCommandStatus.NEW);
    }

    public boolean isInProgress() {
        return commandStatus.equals(ModelCommandStatus.IN_PROGRESS);
    }

    public Integer getCommandId() {
        return commandId;
    }

    public void setCommandId(Integer commandId) {
        this.commandId = commandId;
    }

    public ModelCommandStep getModelCommandStep() {
        return modelCommandStep;
    }

    public void setModelCommandStep(ModelCommandStep modelCommandStep) {
        this.modelCommandStep = modelCommandStep;
    }

}
