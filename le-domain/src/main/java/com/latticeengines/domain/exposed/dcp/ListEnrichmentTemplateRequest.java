package com.latticeengines.domain.exposed.dcp;

public class ListEnrichmentTemplateRequest {

    private String tenantId;

    private String domain;

    private String recordType;

    private Boolean includeArchived;

    private String createdBy;

    public ListEnrichmentTemplateRequest() {
    }

    public ListEnrichmentTemplateRequest(String tenantId, String domain, String recordType, Boolean includeArchived,
            String createdBy) {
        this.tenantId = tenantId;
        this.domain = domain;
        this.recordType = recordType;
        this.includeArchived = includeArchived;
        this.createdBy = createdBy;
    }

    public String getTenantId() {
        return tenantId;
    }

    public String getDomain() {
        return domain;
    }

    public String getRecordType() {
        return recordType;
    }

    public Boolean getIncludeArchived() {
        return includeArchived;
    }

    public String getCreatedBy() {
        return createdBy;
    }
}
