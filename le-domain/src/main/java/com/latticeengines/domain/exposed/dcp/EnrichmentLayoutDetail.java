package com.latticeengines.domain.exposed.dcp;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.manage.DataDomain;
import com.latticeengines.domain.exposed.datacloud.manage.DataRecordType;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class EnrichmentLayoutDetail {

    @JsonProperty("layoutId")
    private String layoutId;

    @JsonProperty("sourceId")
    private String sourceId;

    @JsonProperty("domain")
    private DataDomain domain;

    @JsonProperty("recordType")
    private DataRecordType recordType;

    @JsonProperty("created")
    private Date created;

    @JsonProperty("createdBy")
    private String createdBy;

    @JsonProperty("teamId")
    private String teamId;

    @JsonProperty("updated")
    private Date updated;

    @JsonProperty("tenantId")
    private String tenantId;

    @JsonProperty("elements")
    private List<String> elements;

    private EnrichmentLayoutDetail() {

    }

    public EnrichmentLayoutDetail(EnrichmentLayout enrichmentLayout) {
        this.layoutId =   enrichmentLayout.getLayoutId();
        this.sourceId =   enrichmentLayout.getSourceId();
        this.domain =     enrichmentLayout.getDomain();
        this.recordType = enrichmentLayout.getRecordType();
        this.created =    enrichmentLayout.getCreated();
        this.createdBy =  enrichmentLayout.getCreatedBy();
        this.teamId =     enrichmentLayout.getTeamId();
        this.updated =    enrichmentLayout.getUpdated();
        this.tenantId =   enrichmentLayout.getTenant().getId();
        if (enrichmentLayout.getElements() != null) {
            this.elements = new ArrayList<>(enrichmentLayout.getElements());
        }
    }

    public String getLayoutId() {
        return layoutId;
    }

    public void setLayoutId(String layoutId) {
        this.layoutId = layoutId;
    }

    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    public DataDomain getDomain() {
        return domain;
    }

    public void setDomain(DataDomain domain) {
        this.domain = domain;
    }

    public DataRecordType getRecordType() {
        return recordType;
    }

    public void setRecordType(DataRecordType recordType) {
        this.recordType = recordType;
    }

    public Date getCreated() {
        return created;
    }

    public void setCreated(Date created) {
        this.created = created;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public String getTeamId() {
        return teamId;
    }

    public void setTeamId(String teamId) {
        this.teamId = teamId;
    }

    public Date getUpdated() {
        return updated;
    }

    public void setUpdated(Date updated) {
        this.updated = updated;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenant(String tenantId) {
        this.tenantId = tenantId;
    }

    public List<String> getElements() {
        return elements;
    }

    public void setElements(List<String> elements) {
        this.elements = elements;
    }
}
