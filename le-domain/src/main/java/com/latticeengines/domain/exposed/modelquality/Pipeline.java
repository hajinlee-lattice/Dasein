package com.latticeengines.domain.exposed.modelquality;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.annotation.MetricTag;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Table(name = "MODELQUALITY_PIPELINE", uniqueConstraints = { @UniqueConstraint(columnNames = { "NAME" }) })
@JsonIgnoreProperties({ "hibernateLazyInitializer", "handler" })
public class Pipeline implements HasName, HasPid, Fact, Dimension, Serializable, SupportsLatest {

    private static final long serialVersionUID = 1391478020765752403L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Column(name = "NAME", unique = true, nullable = false)
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

    @JsonProperty("description")
    @Column(name = "DESCRIPTION", nullable = true)
    private String description;

    @JsonIgnore
    @Column(name = "VERSION", nullable = true)
    private Integer version;

    @JsonIgnore
    @OneToMany(fetch = FetchType.EAGER, cascade = CascadeType.ALL, mappedBy = "pk.pipeline")
    @Fetch(FetchMode.SUBSELECT)
    private List<PipelineToPipelineSteps> pipelineSteps = new ArrayList<>();

    @Override
    @MetricTag(tag = "PipelineName")
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

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public Integer getVersion() {
        return version;
    }

    @Override
    public void setVersion(Integer version) {
        this.version = version;
    }

    @JsonProperty("pipeline_steps")
    public List<PipelineStep> getPipelineSteps() {
        List<PipelineStep> steps = new ArrayList<>();

        class CompareSortOrder implements Comparator<PipelineStep> {
            @Override
            public int compare(PipelineStep step1, PipelineStep step2) {
                int a = step1.getSortKey();
                int b = step2.getSortKey();
                return a > b ? +1 : a < b ? -1 : 0;
            }
        }

        for (PipelineToPipelineSteps p : pipelineSteps) {
            PipelineStep step = p.getPipelineStep();
            step.setSortKey(p.getOrder());
            steps.add(step);

        }
        Collections.sort(steps, new CompareSortOrder());
        return steps;
    }

    @JsonProperty("pipeline_steps")
    public void setPipelineSteps(List<PipelineStep> pipelineSteps) {
        for (PipelineStep step : pipelineSteps) {
            addPipelineStep(step);
        }
    }

    public void addPipelineStep(PipelineStep pipelineStep) {
        PipelineToPipelineSteps p = new PipelineToPipelineSteps();
        p.setPipeline(this);
        p.setPipelineStep(pipelineStep);
        pipelineStep.addPipelineToPipelineStep(p);
        pipelineSteps.add(p);
    }

    public void addStepsFromPipelineJson(String pipelineContents) {
        PipelineJson json = JsonUtils.deserialize(pipelineContents, PipelineJson.class);
        Map<String, PipelineStep> steps = json.getSteps();

        for (Map.Entry<String, PipelineStep> entry : steps.entrySet()) {
            addPipelineStep(entry.getValue());
        }

    }

    @JsonIgnore
    public void setPipelineToPipelineSteps(List<PipelineToPipelineSteps> pipelineSteps) {
        this.pipelineSteps = pipelineSteps;
    }

    @JsonIgnore
    public List<PipelineToPipelineSteps> getPipelineToPipelineSteps() {
        return pipelineSteps;
    }
}
