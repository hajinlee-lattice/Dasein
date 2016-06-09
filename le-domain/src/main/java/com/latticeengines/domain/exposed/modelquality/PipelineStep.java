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
import javax.persistence.ManyToMany;
import javax.persistence.OneToMany;
import javax.persistence.Table;

import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

/**
 * 
 * @startuml
 *
 */
@Entity
@Table(name = "MODELQUALITY_PIPELINE_STEP")
@JsonIgnoreProperties({"hibernateLazyInitializer", "handler"})
public class PipelineStep implements HasName, HasPid {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;
    
    @Column(name = "NAME", nullable = false)
    private String name;
    
    @Column(name = "SCRIPT", unique=true, nullable = false)
    private String script;
    
    @ManyToMany(fetch = FetchType.LAZY, cascade = CascadeType.ALL, mappedBy = "pipelineSteps")
    @JsonIgnore
    private List<Pipeline> pipelines = new ArrayList<>();

    @JsonProperty("pipeline_property_defs")
    @OneToMany(cascade = CascadeType.ALL, fetch = FetchType.LAZY, mappedBy = "pipelineStep")
    @OnDelete(action = OnDeleteAction.CASCADE)
    private List<PipelinePropertyDef> pipelinePropertyDefs = new ArrayList<>();

    @Override
    public String getName() {
        return name;
    }
    
    @Override
    public void setName(String name) {
        this.name = name;
    }

    public String getScript() {
        return script;
    }

    public void setScript(String script) {
        this.script = script;
    }

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }
    
    public void addPipeline(Pipeline pipeline) {
        pipelines.add(pipeline);
    }

    public List<PipelinePropertyDef> getPipelinePropertyDefs() {
        return pipelinePropertyDefs;
    }

    public void setPipelinePropertyDefs(List<PipelinePropertyDef> pipelinePropertyDefs) {
        this.pipelinePropertyDefs = pipelinePropertyDefs;
    }
    
    public void addPipelinePropertyDef(PipelinePropertyDef pipelinePropertyDef) {
        pipelinePropertyDefs.add(pipelinePropertyDef);
        pipelinePropertyDef.setPipelineStep(this);
    }

}
