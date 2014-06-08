package com.latticeengines.domain.exposed.dataplatform.dlorchestration;

import com.latticeengines.domain.exposed.dataplatform.HasId;

public class EventOutgoing implements HasId<Integer> {
    
    private int id;
    private int commandId;
    private String jsonPath;
    private String algorithm;
    private int sampleSize;
    
    @Override
    public Integer getId() {        
        return id;
    }
    
    @Override
    public void setId(Integer id) {
        this.id = id;       
    }

    public int getCommandId() {
        return commandId;
    }

    public void setCommandId(int commandId) {
        this.commandId = commandId;
    }

    public String getJsonPath() {
        return jsonPath;
    }

    public void setJsonPath(String jsonPath) {
        this.jsonPath = jsonPath;
    }

    public String getAlgorithm() {
        return algorithm;
    }

    public void setAlgorithm(String algorithm) {
        this.algorithm = algorithm;
    }

    public int getSampleSize() {
        return sampleSize;
    }

    public void setSampleSize(int sampleSize) {
        this.sampleSize = sampleSize;
    }
   
}
