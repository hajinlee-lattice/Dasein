package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonProperty;

public class FilePayload {

    @JsonProperty("file_path")
    public String filePath;
    
    @JsonProperty("application_id")
    public String applicationId;
    
    @JsonProperty("customer_space")
    public String customerSpace;
}
