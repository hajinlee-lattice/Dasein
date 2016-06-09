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
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.transform.TransformationGroup;

@Entity
@Table(name = "MODELQUALITY_DATAFLOW")
@JsonIgnoreProperties({"hibernateLazyInitializer", "handler"})
public class DataFlow implements HasName, HasPid {

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
    
    @JsonProperty("transform_group")
    @Column(name = "TRANSFORM_GROUP", nullable = false)
    private TransformationGroup transformationGroup;

    public Boolean getMatch() {
        return match;
    }
    
    public void setMatch(Boolean match) {
        this.match = match;
    }
    
    public TransformationGroup getTransformationGroup() {
        return transformationGroup;
    }
    
    public void setTransformationGroup(TransformationGroup transformationGroup) {
        this.transformationGroup = transformationGroup;
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
