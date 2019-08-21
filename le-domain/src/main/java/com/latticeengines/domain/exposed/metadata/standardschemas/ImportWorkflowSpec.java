package com.latticeengines.domain.exposed.metadata.standardschemas;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;


public class ImportWorkflowSpec extends FieldDefinitionsRecord {

    @JsonProperty(required = false)
    protected String systemType;

    @JsonProperty(required = false)
    protected String systemObject;

    public String getSystemType() {
        return systemType;
    }

    public void setSystemType(String systemType) {
        this.systemType = systemType;
    }

    public String getSystemObject() {
        return systemObject;
    }

    public void setSystemObject(String systemObject) {
        this.systemObject = systemObject;
    }
}


