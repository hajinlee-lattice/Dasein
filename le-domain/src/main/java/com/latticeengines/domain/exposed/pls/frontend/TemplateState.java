package com.latticeengines.domain.exposed.pls.frontend;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TemplateState {

    // Tenant for which to create a template.
    @JsonProperty
    private String tenantId;

    // The specific name of the (external) system for which a template is being created.
    @JsonProperty
    private String systemName;

    // The type of system for which a template is being created.  This should match the name of a Spec.
    // eg. Salesforce Contacts".
    @JsonProperty
    private String systemType;

    // The current page number being served in the import template workflow.
    @JsonProperty
    private Integer pageNumber;

    // The CSV file for which the template is being constructed.
    @JsonProperty
    private String importFile;

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public String getSystemName() {
        return systemName;
    }

    public void setSystemName(String systemName) {
        this.systemName = systemName;
    }

    public String getSystemType() {
        return systemType;
    }

    public void setSystemType(String systemType) {
        this.systemType = systemType;
    }

    public Integer getPageNumber() {
        return pageNumber;
    }

    public void setPageNumber(Integer pageNumber) {
        this.pageNumber = pageNumber;
    }

    public String getImportFile() {
        return importFile;
    }

    public void setImportFile(String importFile) {
        this.importFile = importFile;
    }

    @Override
    public String toString() {
        String output = "tenantId = " + tenantId + "\nsystemName = " + systemName + "\nsystemType = " + systemType
                + "\npageNumber = " + pageNumber + "\nimportFile = " + importFile;
        return output;
    }
}
