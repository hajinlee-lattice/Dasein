package com.latticeengines.domain.exposed.modelquality;

import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToOne;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Table(name = "MODELQUALITY_ANALYTIC_PIPELINE")
@JsonIgnoreProperties({ "hibernateLazyInitializer", "handler" })
public class AnalyticPipeline implements HasName, HasPid {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonProperty("name")
    @Column(name = "NAME", nullable = false)
    private String name;
    
    @JsonProperty("pipeline")
    @JoinColumn(name = "FK_PIPELINE_ID", nullable = false)
    @OneToOne(cascade = { CascadeType.REMOVE }, fetch = FetchType.LAZY)
    private Pipeline pipeline;

    @JsonProperty("algorithm")
    @JoinColumn(name = "FK_ALGORITHM_ID", nullable = false)
    @OneToOne(cascade = { CascadeType.REMOVE }, fetch = FetchType.LAZY)
    private Algorithm algorithm;

    @JsonProperty("prop_data")
    @JoinColumn(name = "FK_PROPDATA_ID", nullable = false)
    @OneToOne(cascade = { CascadeType.REMOVE }, fetch = FetchType.LAZY)
    private PropData propData;

    @JsonProperty("data_flow")
    @JoinColumn(name = "FK_DATAFLOW_ID", nullable = false)
    @OneToOne(cascade = { CascadeType.REMOVE }, fetch = FetchType.LAZY)
    private DataFlow dataFlow;

    @JsonProperty("sampling")
    @JoinColumn(name = "FK_SAMPLING_ID", nullable = false)
    @OneToOne(cascade = { CascadeType.REMOVE }, fetch = FetchType.LAZY)
    private Sampling sampling;

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
    
    public void setPipeline(Pipeline pipeline) {
        this.pipeline = pipeline;
    }
    
    public Pipeline getPipeline() {
        return pipeline;
    }

    public void setAlgorithm(Algorithm algorithm) {
        this.algorithm = algorithm;
    }
    
    public Algorithm getAlgorithm() {
        return algorithm;
    }
    
    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    public PropData getPropData() {
        return propData;
    }

    public void setPropData(PropData propData) {
        this.propData = propData;
    }

    public DataFlow getDataFlow() {
        return dataFlow;
    }

    public void setDataFlow(DataFlow dataFlow) {
        this.dataFlow = dataFlow;
    }

    public Sampling getSampling() {
        return sampling;
    }

    public void setSampling(Sampling sampling) {
        this.sampling = sampling;
    }

}
