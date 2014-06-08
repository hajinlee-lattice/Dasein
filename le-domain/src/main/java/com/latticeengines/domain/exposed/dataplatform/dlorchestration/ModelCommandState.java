package com.latticeengines.domain.exposed.dataplatform.dlorchestration;

import com.latticeengines.domain.exposed.dataplatform.HasId;


public class ModelCommandState implements HasId<Integer> {

    private int id;
    private int commandId;
    private ModelCommandStep modelCommandStep;
    private String yarnApplicationId;
    private float progress;
    private String diagnostics;
    private String trackingUrl;
    private long elapsedTimeInMillis;
    
    @Override
    public Integer getId() {
        return id;
    }

    @Override
    public void setId(Integer id) {
        this.id = id;
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

    public String getYarnApplicationId() {
        return yarnApplicationId;
    }

    public void setYarnApplicationId(String yarnApplicationId) {
        this.yarnApplicationId = yarnApplicationId;
    }

    public float getProgress() {
        return progress;
    }

    public void setProgress(float progress) {
        this.progress = progress;
    }

    public String getDiagnostics() {
        return diagnostics;
    }

    public void setDiagnostics(String diagnostics) {
        this.diagnostics = diagnostics;
    }

    public String getTrackingUrl() {
        return trackingUrl;
    }

    public void setTrackingUrl(String trackingUrl) {
        this.trackingUrl = trackingUrl;
    }

    public long getElapsedTimeInMillis() {
        return elapsedTimeInMillis;
    }

    public void setElapsedTimeInMillis(long elapsedTimeInMillis) {
        this.elapsedTimeInMillis = elapsedTimeInMillis;
    }
}
