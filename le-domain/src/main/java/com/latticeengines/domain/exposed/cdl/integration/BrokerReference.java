package com.latticeengines.domain.exposed.cdl.integration;


import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.IngestionScheduler;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect( //
        fieldVisibility = JsonAutoDetect.Visibility.NONE, //
        getterVisibility = JsonAutoDetect.Visibility.NONE, //
        isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
        setterVisibility = JsonAutoDetect.Visibility.NONE //
)
public class BrokerReference {

    @JsonProperty("dataStreamId")
    private String dataStreamId;

    @JsonProperty("active")
    private Boolean active;

    @JsonProperty("sourceId")
    private String sourceId;

    @JsonProperty("selectedFields")
    private List<String> selectedFields;

    @JsonProperty("connectionType")
    private InboundConnectionType connectionType;

    @JsonProperty("scheduler")
    private IngestionScheduler scheduler;

    @JsonProperty("documentType")
    private String documentType;

    @JsonProperty("lastAggregationWorkflowId")
    private Long lastAggregationWorkflowId;

    public Boolean getActive() {
        return active;
    }

    public void setActive(Boolean active) {
        this.active = active;
    }

    public List<String> getSelectedFields() {
        return selectedFields;
    }

    public void setSelectedFields(List<String> selectedFields) {
        this.selectedFields = selectedFields;
    }

    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    public InboundConnectionType getConnectionType() {
        return connectionType;
    }

    public void setConnectionType(InboundConnectionType connectionType) {
        this.connectionType = connectionType;
    }


    public String getDataStreamId() {
        return dataStreamId;
    }

    public void setDataStreamId(String dataStreamId) {
        this.dataStreamId = dataStreamId;
    }

    public String getDocumentType() {
        return documentType;
    }

    public void setDocumentType(String documentType) {
        this.documentType = documentType;
    }

    public IngestionScheduler getScheduler() {
        return scheduler;
    }

    public void setScheduler(IngestionScheduler scheduler) {
        this.scheduler = scheduler;
    }

    public Long getLastAggregationWorkflowId() {
        return lastAggregationWorkflowId;
    }

    public void setLastAggregationWorkflowId(Long lastAggregationWorkflowId) {
        this.lastAggregationWorkflowId = lastAggregationWorkflowId;
    }
}
