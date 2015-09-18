package com.latticeengines.eai.appmaster.service;

import org.springframework.yarn.integration.ip.mind.binding.BaseResponseObject;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

public class AppMasterServiceResponse extends BaseResponseObject  {
    
    public AppMasterServiceResponse(){
        
    }
    
    public AppMasterServiceResponse(String mess){
        this.mess = mess;
    }
    private String mess;
    
    @JsonProperty("mess")
    public String getMessage(){
        return this.mess;
    }
    
    @JsonProperty("mess")
    public void setMessage(String mess){
        this.mess = mess;
    }

    public String toString(){
        return JsonUtils.serialize(this);
    }
}
