package com.latticeengines.domain.exposed.metadata.standardschemas;

import org.apache.commons.lang3.StringUtils;

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

    @Override
    public boolean equals(Object object) {
        if (object instanceof ImportWorkflowSpec) {
            ImportWorkflowSpec spec = (ImportWorkflowSpec) object;

            if (!StringUtils.equals(this.systemType, spec.systemType) ||
                !StringUtils.equals(this.systemObject, spec.systemObject)) {
                return false;
            }
            return super.equals(spec);
        }
        return false;
    }
}


