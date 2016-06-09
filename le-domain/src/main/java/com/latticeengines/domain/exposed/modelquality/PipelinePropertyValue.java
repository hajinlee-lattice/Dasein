package com.latticeengines.domain.exposed.modelquality;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

/**
 * 
 * @startuml
 *
 */
@Entity
@Table(name = "MODELQUALITY_PIPELINE_PROPERTY_VALUE")
@JsonIgnoreProperties({"hibernateLazyInitializer", "handler"})
public class PipelinePropertyValue implements HasPid {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Column(name = "VALUE", nullable = false)
    private String value;
    
    @ManyToOne
    @JoinColumn(name = "FK_PIPELINE_PROPDEF_ID", nullable = false)
    @JsonIgnore
    private PipelinePropertyDef pipelinePropertyDef;
    
    public PipelinePropertyValue() {}
    
    public PipelinePropertyValue(String value) {
        setValue(value);
    }
    
    public String getValue() {
        return value;
    }
    
    public void setValue(String value) {
        this.value = value;
    }

    public PipelinePropertyDef getPipelinePropertyDef() {
        return pipelinePropertyDef;
    }

    public void setPipelinePropertyDef(PipelinePropertyDef pipelinePropertyDef) {
        this.pipelinePropertyDef = pipelinePropertyDef;
    }

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }
}
