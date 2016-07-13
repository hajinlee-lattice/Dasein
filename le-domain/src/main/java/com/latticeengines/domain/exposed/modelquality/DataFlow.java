package com.latticeengines.domain.exposed.modelquality;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.annotation.MetricTag;
import com.latticeengines.domain.exposed.dataflow.flows.leadprioritization.DedupType;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.transform.TransformationGroup;

@Entity
@Table(name = "MODELQUALITY_DATAFLOW")
@JsonIgnoreProperties({ "hibernateLazyInitializer", "handler" })
public class DataFlow implements HasName, HasPid, Fact, Dimension {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Column(name = "NAME", nullable = false)
    private String name;

    @Column(name = "MATCH", nullable = false)
    private Boolean match;

    @JsonProperty("exclude_propdata_columns")
    @Column(name = "EXCLUDE_PROPDATA_COLUMNS", nullable = true)
    private boolean excludePropDataColumns = false;

    @JsonProperty("transform_group")
    @Column(name = "TRANSFORM_GROUP", nullable = true)
    private TransformationGroup transformationGroup;

    @JsonProperty("transform_dedup_type")
    @Column(name = "TRANSFORM_DEDUP_TYPE", nullable = true)
    private DedupType dedupType;

    @JsonProperty("predefined_selection_name")
    @Column(name = "PREDEFINED_SELECTION_NAME", nullable = true)
    private String predefinedSelectionName;

    public Boolean getMatch() {
        return match;
    }

    public void setMatch(Boolean match) {
        this.match = match;
    }

    public TransformationGroup getTransformationGroup() {
        return transformationGroup;
    }

    public DedupType getDedupType() {
        return dedupType;
    }

    public void setDedupType(DedupType dedupType) {
        this.dedupType = dedupType;
    }

    public void setTransformationGroup(TransformationGroup transformationGroup) {
        this.transformationGroup = transformationGroup;
    }

    public boolean isExcludePropDataColumns() {
        return excludePropDataColumns;
    }

    public void setExcludePropDataColumns(boolean excludePropDataColumns) {
        this.excludePropDataColumns = excludePropDataColumns;
    }

    @MetricTag(tag = "ExcludePropDataColumns")
    @JsonIgnore
    public String getExcludePropDataColumns() {
        return String.valueOf(excludePropDataColumns);
    }

    @MetricTag(tag = "TransformationGroupName")
    @JsonIgnore
    public String getTransformationGroupStrValue() {
        return transformationGroup.getName();
    }

    @MetricTag(tag = "DedupType")
    @JsonIgnore
    public String getDedupTypeStrValue() {
        return dedupType.name();
    }

    @MetricTag(tag = "PredefinedSelectionName")
    public String getPredefinedSelectionName() {
        return predefinedSelectionName;
    }

    public void setPredefinedSelectionName(String predefinedSelectionName) {
        this.predefinedSelectionName = predefinedSelectionName;
    }

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }
}
