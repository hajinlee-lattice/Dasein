package com.latticeengines.domain.exposed.admin;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DLRestResult {
    private String errorMessage;

    private int status;

    private List<ValueResult> value;

    @JsonProperty("ErrorMessage")
    public String getErrorMessage(){
        return this.errorMessage;
    }
    
    public void setErrorMessage(String errorMessage){
        this.errorMessage = errorMessage;
    }

    @JsonProperty("Status")
    public int getStatus(){
        return this.status;
    }

    public void setStatus(int status){
        this.status = status;
    }

    @JsonProperty("Value")
    public List<ValueResult> getValueResult(){
        return this.value;
    }

    public void setValueResult(List<ValueResult> value){
        this.value = value;
    }
 
    public static class ValueResult{

        private String key;

        private String value;

        @JsonProperty("Key")
        public String getKey(){
            return this.key;
        }

        public void setKey(String key){
            this.key = key;
        }

        @JsonProperty("Value")
        public String getValue(){
            return this.value;
        }

        public void setValue(String value){
            this.value = value;
        }
    }
}
