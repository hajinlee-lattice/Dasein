package com.latticeengines.domain.exposed.scoringapi;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;

import io.swagger.annotations.ApiModelProperty;

public class ModelDetail {
    @JsonProperty("model")
    @ApiModelProperty(value = "Model")
    private Model model;

    @JsonProperty("status")
    @ApiModelProperty(value = "Model status enum (Active, Deactive, Deleted)")
    private ModelSummaryStatus status;

    @JsonProperty("fields")
    @ApiModelProperty(value = "Model fields")
    private Fields fields;

    @JsonProperty("lastModifiedTimestamp")
    @ApiModelProperty(value = "Last modified timestamp")
    private Long lastModifiedTimestamp;

    public ModelDetail() {
        super();
    }

    public ModelDetail(Model model, ModelSummaryStatus status, Fields fields, Long lastModifiedTimestamp) {
        this();
        this.model = model;
        this.status = status;
        this.fields = fields;
        this.lastModifiedTimestamp = lastModifiedTimestamp;
    }

    public Model getModel() {
        return model;
    }

    public void setModel(Model model) {
        this.model = model;
    }

    public ModelSummaryStatus getStatus() {
        return status;
    }

    public void setStatus(ModelSummaryStatus status) {
        this.status = status;
    }

    public Fields getFields() {
        return fields;
    }

    public void setFields(Fields fields) {
        this.fields = fields;
    }

    public Long getLastModifiedTimestamp() {
        return lastModifiedTimestamp;
    }

    public void setLastModifiedTimestamp(Long lastModifiedTimestamp) {
        this.lastModifiedTimestamp = lastModifiedTimestamp;
    }
}
