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
import javax.persistence.OneToMany;
import javax.persistence.Table;

import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.annotation.MetricTag;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

/**
 * 
 * @startuml
 * 
 */
@Entity
@Table(name = "MODELQUALITY_ALGORITHM")
@JsonIgnoreProperties({ "hibernateLazyInitializer", "handler" })
public class Algorithm implements HasName, HasPid, Fact, Dimension {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Column(name = "NAME", nullable = false)
    private String name;

    @Column(name = "SCRIPT", nullable = false)
    private String script;

    @JsonProperty("algorithm_property_defs")
    @OneToMany(cascade = CascadeType.ALL, fetch = FetchType.EAGER, mappedBy = "algorithm")
    @OnDelete(action = OnDeleteAction.CASCADE)
    private List<AlgorithmPropertyDef> algorithmPropertyDefs = new ArrayList<>();

    @Override
    @MetricTag(tag = "AlgorithmName")
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @MetricTag(tag = "AlgorithmScript")
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

    public List<AlgorithmPropertyDef> getAlgorithmPropertyDefs() {
        return algorithmPropertyDefs;
    }

    public void setAlgorithmPropertyDefs(List<AlgorithmPropertyDef> algorithmPropertyDefs) {
        this.algorithmPropertyDefs = algorithmPropertyDefs;
    }

    public void addAlgorithmPropertyDef(AlgorithmPropertyDef algorithmPropertyDef) {
        algorithmPropertyDefs.add(algorithmPropertyDef);
        algorithmPropertyDef.setAlgorithm(this);
    }

}
