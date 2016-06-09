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
import javax.persistence.ManyToOne;
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
@Table(name = "MODELQUALITY_PIPELINE_PROPERTY_DEF")
@JsonIgnoreProperties({"hibernateLazyInitializer", "handler"})
public class PipelinePropertyDef implements HasName, HasPid {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Column(name = "NAME", nullable = false)
    private String name;
    
    @ManyToOne
    @JoinColumn(name = "FK_PIPELINE_STEP_ID", nullable = false)
    @JsonIgnore
    private PipelineStep pipelineStep;

    @JsonProperty("pipeline_property_values")
    @OneToMany(cascade = CascadeType.ALL, fetch = FetchType.LAZY, mappedBy = "pipelinePropertyDef")
    @OnDelete(action = OnDeleteAction.CASCADE)
    private List<PipelinePropertyValue> pipelinePropertyValues = new ArrayList<>();
    
    public PipelinePropertyDef() {}
    
    public PipelinePropertyDef(String name) {
        setName(name);
    }

    @Override
    public String getName() {
        return name;
    }
    
    @Override
    public void setName(String name) {
        this.name = name;
    }

    public PipelineStep getPipelineStep() {
        return pipelineStep;
    }

    public void setPipelineStep(PipelineStep pipelineStep) {
        this.pipelineStep = pipelineStep;
    }

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public List<PipelinePropertyValue> getPipelinePropertyValues() {
        return pipelinePropertyValues;
    }

    public void setPipelinePropertyValues(List<PipelinePropertyValue> pipelinePropertyValues) {
        this.pipelinePropertyValues = pipelinePropertyValues;
    }
    
    public void addPipelinePropertyValue(PipelinePropertyValue pipelinePropertyValue) {
        pipelinePropertyValues.add(pipelinePropertyValue);
        pipelinePropertyValue.setPipelinePropertyDef(this);
    }
}
