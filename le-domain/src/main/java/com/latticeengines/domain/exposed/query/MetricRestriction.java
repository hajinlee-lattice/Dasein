package com.latticeengines.domain.exposed.query;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class MetricRestriction extends Restriction {

    @JsonProperty("metric_entity")
    private BusinessEntity metricEntity;

    @JsonProperty("restriction")
    private Restriction restriction;

    public MetricRestriction() {
    }

    public MetricRestriction(BusinessEntity metricEntity, Restriction restriction) {
        this.metricEntity = metricEntity;
        this.restriction = restriction;
    }

    public BusinessEntity getMetricEntity() {
        return metricEntity;
    }

    public void setMetricEntity(BusinessEntity metricEntity) {
        this.metricEntity = metricEntity;
    }

    public Restriction getRestriction() {
        return restriction;
    }

    public void setRestriction(Restriction restriction) {
        this.restriction = restriction;
    }
}
