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
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Table(name = "MODELQUALITY_PROPDATA")
@JsonIgnoreProperties({ "hibernateLazyInitializer", "handler" })
public class PropData implements HasName, HasPid, Fact, Dimension {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Column(name = "NAME", nullable = false)
    private String name;

    @JsonProperty("exclude_propdata_columns")
    @Column(name = "EXCLUDE_PROPDATA_COLUMNS", nullable = true)
    private boolean excludePropDataColumns = false;
    
    @JsonProperty("predefined_selection_name")
    @Column(name = "PREDEFINED_SELECTION_NAME", nullable = true)
    private String predefinedSelectionName;

    @Column(name = "VERSION", nullable = false)
    private String version;

    @JsonProperty("data_cloud_version")
    @Column(name = "DATA_CLOUD_VERSION", nullable = false)
    private String dataCloudVersion;

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    public Long getPid() {
        return pid;
    }

    public void setPid(Long pid) {
        this.pid = pid;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    @MetricTag(tag = "DataCloudVersion")
    public String getDataCloudVersion() {
        return dataCloudVersion;
    }

    public void setDataCloudVersion(String dataCloudVersion) {
        this.dataCloudVersion = dataCloudVersion;
    }

    @JsonIgnore
    public boolean isExcludePropDataColumns() {
        return excludePropDataColumns;
    }

    @MetricTag(tag = "ExcludePropDataColumns")
    @JsonIgnore
    public String isExcludePropDataColumnsStrValue() {
        return "" + excludePropDataColumns;
    }
    
    public void setExcludePropDataColumns(boolean excludePropDataColumns) {
        this.excludePropDataColumns = excludePropDataColumns;
    }
    
    @MetricTag(tag = "PredefinedSelectionName")
    public String getPredefinedSelectionName() {
        return predefinedSelectionName;
    }

    public void setPredefinedSelectionName(String predefinedSelectionName) {
        this.predefinedSelectionName = predefinedSelectionName;
    }
}
