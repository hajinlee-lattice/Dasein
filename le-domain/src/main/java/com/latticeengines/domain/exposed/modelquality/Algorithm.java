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
import javax.persistence.UniqueConstraint;

import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;
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
@Table(name = "MODELQUALITY_ALGORITHM", uniqueConstraints = {
        @UniqueConstraint(columnNames = { "NAME" }) })
@JsonIgnoreProperties({ "hibernateLazyInitializer", "handler" })
public class Algorithm implements HasName, HasPid, Fact, Dimension, SupportsLatest {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Column(name = "NAME", unique = true, nullable = false)
    private String name;

    @Column(name = "TYPE", nullable = true)
    private AlgorithmType type;

    @Column(name = "SCRIPT", nullable = false)
    private String script;

    @JsonIgnore
    @Column(name = "VERSION", nullable = true)
    private Integer version;

    @JsonProperty("algorithm_property_defs")
    @OneToMany(cascade = CascadeType.ALL, fetch = FetchType.EAGER, mappedBy = "algorithm")
    @OnDelete(action = OnDeleteAction.CASCADE)
    @Fetch(FetchMode.SUBSELECT)
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

    public AlgorithmType getType() {
        return type;
    }

    public void setType(AlgorithmType type) {
        this.type = type;
    }

    @MetricTag(tag = "AlgorithmType")
    @JsonIgnore
    public String getTypeStrValue() {
        if (type == null)
            return "";
        return type.toString();
    }

    @JsonIgnore
    public String getDataPlatformAlgorithmName() {
        switch (type) {
            case RANDOMFOREST:
                return "RF";
            case LOGISTICREGRESSION:
                return "LR";
            case DECISIONTREE:
                return "DT";
            default:
                return "RF";
        }
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

    @Override
    public Integer getVersion() {
        return version;
    }

    @Override
    public void setVersion(Integer version) {
        this.version = version;
    }

    public List<AlgorithmPropertyDef> getAlgorithmPropertyDefs() {
        return algorithmPropertyDefs;
    }

    public void setAlgorithmPropertyDefs(List<AlgorithmPropertyDef> algorithmPropertyDefs) {
        this.algorithmPropertyDefs = algorithmPropertyDefs;
        for (AlgorithmPropertyDef algorithmPropertyDef : algorithmPropertyDefs) {
            algorithmPropertyDef.setAlgorithm(this);
        }
    }

    public void addAlgorithmPropertyDef(AlgorithmPropertyDef algorithmPropertyDef) {
        algorithmPropertyDefs.add(algorithmPropertyDef);
        algorithmPropertyDef.setAlgorithm(this);
    }

}
