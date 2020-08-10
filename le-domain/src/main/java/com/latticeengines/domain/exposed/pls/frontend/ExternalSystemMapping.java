package com.latticeengines.domain.exposed.pls.frontend;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ExternalSystemMapping {

    @JsonProperty("name_from_file")
    private String nameFromFile;

    @JsonProperty("attr_display_name")
    private String attrDisplayName;

    @JsonProperty("system_type")
    private CDLExternalSystemType systemType;

    @JsonProperty("in_current_template")
    private Boolean inCurrentTemplate;

    public String getNameFromFile() {
        return nameFromFile;
    }

    public void setNameFromFile(String nameFromFile) {
        this.nameFromFile = nameFromFile;
    }

    public String getAttrDisplayName() {
        return attrDisplayName;
    }

    public void setAttrDisplayName(String attrDisplayName) {
        this.attrDisplayName = attrDisplayName;
    }

    public CDLExternalSystemType getSystemType() {
        return systemType;
    }

    public void setSystemType(CDLExternalSystemType systemType) {
        this.systemType = systemType;
    }

    public Boolean getInCurrentTemplate() {
        return inCurrentTemplate;
    }

    public void setInCurrentTemplate(Boolean inCurrentTemplate) {
        this.inCurrentTemplate = inCurrentTemplate;
    }
}
