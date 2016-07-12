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

import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.metric.annotation.MetricTag;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Table(name = "MODELQUALITY_SAMPLING_PROPERTY_DEF")
@JsonIgnoreProperties({ "hibernateLazyInitializer", "handler" })
public class SamplingPropertyDef implements HasName, HasPid {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Column(name = "NAME", nullable = false)
    private String name;

    @ManyToOne
    @JoinColumn(name = "FK_SAMPLING_ID", nullable = false)
    @JsonIgnore
    private Sampling sampling;

    @JsonProperty("sampling_property_values")
    @OneToMany(cascade = CascadeType.ALL, fetch = FetchType.EAGER, mappedBy = "samplingPropertyDef")
    @OnDelete(action = OnDeleteAction.CASCADE)
    @Fetch(value = FetchMode.SUBSELECT)
    private List<SamplingPropertyValue> samplingPropertyValues = new ArrayList<>();

    public SamplingPropertyDef() {
    }

    public SamplingPropertyDef(String name) {
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

    public Sampling getSampling() {
        return sampling;
    }

    public void setSampling(Sampling sampling) {
        this.sampling = sampling;
    }

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public List<SamplingPropertyValue> getSamplingPropertyValues() {
        return samplingPropertyValues;
    }

    public void setSamplingPropertyValues(List<SamplingPropertyValue> samplingPropertyValues) {
        this.samplingPropertyValues = samplingPropertyValues;
    }

    public void addSamplingPropertyValue(SamplingPropertyValue samplingPropertyValue) {
        samplingPropertyValues.add(samplingPropertyValue);
        samplingPropertyValue.setSamplingPropertyDef(this);
    }

    @MetricTag(tagReferencingField = "name")
    public String getSamplingPropertyStrValues() {
        return samplingPropertyValues.toString();
    }

}
