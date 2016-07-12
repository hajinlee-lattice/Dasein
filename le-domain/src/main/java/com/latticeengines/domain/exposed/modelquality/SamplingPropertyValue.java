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

@Entity
@Table(name = "MODELQUALITY_SAMPLING_PROPERTY_VALUE")
@JsonIgnoreProperties({ "hibernateLazyInitializer", "handler" })
public class SamplingPropertyValue implements HasPid {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Column(name = "VALUE")
    private String value;

    @ManyToOne
    @JoinColumn(name = "FK_SAMPLING_PROPDEF_ID", nullable = false)
    @JsonIgnore
    private SamplingPropertyDef samplingPropertyDef;

    public SamplingPropertyValue() {
    }

    public SamplingPropertyValue(String value) {
        setValue(value);
    }

    public SamplingPropertyDef getSamplingPropertyDef() {
        return samplingPropertyDef;
    }

    public void setSamplingPropertyDef(SamplingPropertyDef samplingPropertyDef) {
        this.samplingPropertyDef = samplingPropertyDef;
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

    public String toString() {
        return value;
    }

}
