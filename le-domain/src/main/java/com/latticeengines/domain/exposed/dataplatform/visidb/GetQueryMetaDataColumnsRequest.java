package com.latticeengines.domain.exposed.dataplatform.visidb;

import com.fasterxml.jackson.annotation.JsonProperty;

public class GetQueryMetaDataColumnsRequest {

    private String tenantName;
    private String queryName;
    private String revisionTag;

    public GetQueryMetaDataColumnsRequest(String tenantName, String queryName) {
        this.tenantName = tenantName;
        this.queryName = queryName;
    }

    @JsonProperty("tenantName")
    public String getTenantName() {
        return tenantName;
    }

    @JsonProperty("tenantName")
    public void setTenantName(String tenantName) {
        this.tenantName = tenantName;
    }

    @JsonProperty("queryName")
    public String getQueryName() {
        return queryName;
    }

    @JsonProperty("queryName")
    public void setQueryName(String queryName) {
        this.queryName = queryName;
    }

    @JsonProperty("revisionTag")
    public String getRevisionTag() {
        return revisionTag;
    }

    @JsonProperty("revisionTag")
    public void setRevisionTag(String revisionTag) {
        this.revisionTag = revisionTag;
    }

}
