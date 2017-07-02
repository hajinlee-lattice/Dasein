package com.latticeengines.domain.exposed.modelquality;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.annotation.MetricTag;
import com.latticeengines.common.exposed.metric.annotation.MetricTagGroup;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Table(name = "MODELQUALITY_SAMPLING", uniqueConstraints = { @UniqueConstraint(columnNames = { "NAME" }) })
@JsonIgnoreProperties({ "hibernateLazyInitializer", "handler" })
public class Sampling implements HasName, HasPid, Fact, Dimension, SupportsLatest {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Column(name = "NAME", unique = true, nullable = false)
    private String name;

    @Column(name = "PARALLEL_ENABLED", nullable = false)
    private boolean parallelEnabled = false;
    
    @JsonIgnore
    @Column(name = "VERSION")
    private Integer version;

    @JsonProperty("sampling_property_defs")
    @OneToMany(cascade = CascadeType.ALL, fetch = FetchType.EAGER, mappedBy = "sampling")
    @OnDelete(action = OnDeleteAction.CASCADE)
    @Fetch(FetchMode.SUBSELECT)
    private List<SamplingPropertyDef> samplingPropertyDefs = new ArrayList<>();

    public Sampling() {
    }

    public Sampling(String name) {
        setName(name);
    }

    @Override
    @MetricTag(tag = "SamplingName")
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public boolean isParallelEnabled() {
        return parallelEnabled;
    }

    public void setParallelEnabled(boolean parallelEnabled) {
        this.parallelEnabled = parallelEnabled;
    }

    @MetricTag(tag = "IsParallelSamplingEnabled")
    @JsonIgnore
    public String getParallelEnabled() {
        return String.valueOf(parallelEnabled);
    }
    
    @Override
    public Integer getVersion() {
        return version;
    }

    @Override
    public void setVersion(Integer version) {
        this.version = version;
    }

    @MetricTagGroup
    public List<SamplingPropertyDef> getSamplingPropertyDefs() {
        return samplingPropertyDefs;
    }

    public void setSamplingPropertyDefs(List<SamplingPropertyDef> samplingPropertyDefs) {
        this.samplingPropertyDefs = samplingPropertyDefs;
        for (SamplingPropertyDef samplingPropertyDef : samplingPropertyDefs) {
            samplingPropertyDef.setSampling(this);
        }
    }

    public void addSamplingPropertyDef(SamplingPropertyDef samplingPropertyDef) {
        samplingPropertyDefs.add(samplingPropertyDef);
        samplingPropertyDef.setSampling(this);
    }

}
