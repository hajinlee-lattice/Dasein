package com.latticeengines.domain.exposed.scoring;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ScoringConfiguration {

    private String customer;

    private String sourceDataDir;

    private String targetResultDir;

    private List<String> modelGuids  = new ArrayList<>();

    @JsonProperty("customer")
    public String getCustomer(){
        return customer;
    }

    @JsonProperty("customer")
    public void setCustomer(String customer){
        this.customer = customer;
    }

    @JsonProperty("source_data_dir")
    public String getSourceDataDir(){
        return sourceDataDir;
    }

    @JsonProperty("source_data_dir")
    public void setSourceDataDir(String sourceDataDir){
        this.sourceDataDir = sourceDataDir;
    }

    @JsonProperty("target_result_dir")
    public String getTargetResultDir(){
        return targetResultDir;
    }

    @JsonProperty("target_result_dir")
    public void setTargetResultDir(String targetResultDir){
        this.targetResultDir = targetResultDir;
    }

    @JsonProperty("model_guids")
    public List<String> getModelGuids(){
        return modelGuids;
    }

    @JsonProperty("model_guids")
    public void setModelGuids(List<String> modelGuids){
        this.modelGuids = modelGuids;
    }
}
