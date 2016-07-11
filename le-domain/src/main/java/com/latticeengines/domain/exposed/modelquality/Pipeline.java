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
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Table(name = "MODELQUALITY_PIPELINE")
@JsonIgnoreProperties({ "hibernateLazyInitializer", "handler" })
public class Pipeline implements HasName, HasPid, Fact, Dimension {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Column(name = "NAME", nullable = false)
    private String name;

    @JsonProperty("pipeline_script")
    @Column(name = "PIPELINE_SCRIPT", nullable = true)
    private String pipelineScript;

    @JsonProperty("pipeline_lib_script")
    @Column(name = "PIPELINE_LIB_SCRIPT", nullable = true)
    private String pipelineLibScript;

    @JsonProperty("pipeline_driver")
    @Column(name = "PIPELINE_DRIVER", nullable = true)
    private String pipelineDriver;

    @JsonProperty("pipeline_steps")
    @ManyToMany(fetch = FetchType.EAGER, cascade = { CascadeType.ALL })
    @JoinTable(name = "MODELQUALITY_PIPELINE_PIPELINE_STEP", //
    joinColumns = { @JoinColumn(name = "PIPELINE_ID") }, //
    inverseJoinColumns = { @JoinColumn(name = "PIPELINE_STEP_ID") })
    private List<PipelineStep> pipelineSteps = new ArrayList<>();

    @Override
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

    public String getPipelineScript() {
        return pipelineScript;
    }

    public void setPipelineScript(String pipelineScript) {
        this.pipelineScript = pipelineScript;
    }

    public String getPipelineLibScript() {
        return pipelineLibScript;
    }

    public void setPipelineLibScript(String pipelineLibScript) {
        this.pipelineLibScript = pipelineLibScript;
    }

    public String getPipelineDriver() {
        return pipelineDriver;
    }

    public void setPipelineDriver(String pipelineDriver) {
        this.pipelineDriver = pipelineDriver;
    }

    public List<PipelineStep> getPipelineSteps() {
        return pipelineSteps;
    }

    public void setPipelineSteps(List<PipelineStep> pipelineSteps) {
        this.pipelineSteps = pipelineSteps;
    }

    public void addPipelineStep(PipelineStep pipelineStep) {
        pipelineSteps.add(pipelineStep);
        pipelineStep.addPipeline(this);
    }
}
