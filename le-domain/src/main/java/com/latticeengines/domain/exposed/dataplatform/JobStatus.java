package com.latticeengines.domain.exposed.dataplatform;

import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;


public class JobStatus implements HasId<String> {

    private String id;
    private FinalApplicationStatus state;
    private String resultDirectory;

    @Override
    public String getId() {
        return id;
    }

    @Override
    public void setId(String id) {
        this.id = id;
    }

    public FinalApplicationStatus getState() {
        return state;
    }

    public void setState(FinalApplicationStatus state) {
        this.state = state;
    }

    public String getResultDirectory() {
        return resultDirectory;
    }

    public void setResultDirectory(String resultDirectory) {
        this.resultDirectory = resultDirectory;
    }

}
