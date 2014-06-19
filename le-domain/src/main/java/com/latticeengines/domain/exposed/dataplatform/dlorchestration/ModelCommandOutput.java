package com.latticeengines.domain.exposed.dataplatform.dlorchestration;

import java.io.Serializable;
import java.util.Date;

public class ModelCommandOutput implements Serializable {

    private static final long serialVersionUID = 1L;

    private int id;
    
    private int commandId;
    
    private int sampleSize;
    
    private String algorithm;
    
    private String jsonPath;
    
    private Date timestamp;

    public ModelCommandOutput(int id, int commandId, int sampleSize, String algorithm, String jsonPath, Date timestamp) {
        super();
        this.id = id;
        this.commandId = commandId;
        this.sampleSize = sampleSize;
        this.algorithm = algorithm;
        this.jsonPath = jsonPath;
        this.timestamp = timestamp;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getCommandId() {
        return commandId;
    }

    public void setCommandId(int commandId) {
        this.commandId = commandId;
    }

    public int getSampleSize() {
        return sampleSize;
    }

    public void setSampleSize(int sampleSize) {
        this.sampleSize = sampleSize;
    }

    public String getAlgorithm() {
        return algorithm;
    }

    public void setAlgorithm(String algorithm) {
        this.algorithm = algorithm;
    }

    public String getJsonPath() {
        return jsonPath;
    }

    public void setJsonPath(String jsonPath) {
        this.jsonPath = jsonPath;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }
}
