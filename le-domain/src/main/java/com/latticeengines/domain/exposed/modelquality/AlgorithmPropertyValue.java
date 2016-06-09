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
@Table(name = "MODELQUALITY_ALGORITHM_PROPERTY_VALUE")
@JsonIgnoreProperties({"hibernateLazyInitializer", "handler"})
public class AlgorithmPropertyValue implements HasPid {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Column(name = "VALUE")
    private String value;
    
    @ManyToOne
    @JoinColumn(name = "FK_ALGORITHM_PROPDEF_ID", nullable = false)
    private AlgorithmPropertyDef algorithmPropertyDef;
    
    public AlgorithmPropertyValue() {}
    
    public AlgorithmPropertyValue(String value) {
        setValue(value);
    }
    
    public AlgorithmPropertyDef getAlgorithmPropertyDef() {
        return algorithmPropertyDef;
    }

    public void setAlgorithmPropertyDef(AlgorithmPropertyDef algorithmPropertyDef) {
        this.algorithmPropertyDef = algorithmPropertyDef;
    }

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

}
