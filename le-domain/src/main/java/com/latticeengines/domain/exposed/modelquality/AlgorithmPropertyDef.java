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
@Table(name = "MODELQUALITY_ALGORITHM_PROPERTY_DEF")
@JsonIgnoreProperties({"hibernateLazyInitializer", "handler"})
public class AlgorithmPropertyDef implements HasName, HasPid {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Column(name = "NAME", nullable = false)
    public String name;
    
    @ManyToOne
    @JoinColumn(name = "FK_ALGORITHM_ID", nullable = false)
    private Algorithm algorithm;

    @JsonProperty("algorithm_property_values")
    @OneToMany(cascade = CascadeType.ALL, fetch = FetchType.LAZY, mappedBy = "algorithmPropertyDef")
    @OnDelete(action = OnDeleteAction.CASCADE)
    private List<AlgorithmPropertyValue> algorithmPropertyValues = new ArrayList<>();
    
    public AlgorithmPropertyDef() {}
    
    public AlgorithmPropertyDef(String name) {
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
    
    public Algorithm getAlgorithm() {
        return algorithm;
    }

    public void setAlgorithm(Algorithm algorithm) {
        this.algorithm = algorithm;
    }

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public List<AlgorithmPropertyValue> getAlgorithmPropertyValues() {
        return algorithmPropertyValues;
    }

    public void setAlgorithmPropertyValues(List<AlgorithmPropertyValue> algorithmPropertyValues) {
        this.algorithmPropertyValues = algorithmPropertyValues;
    }
    
    public void addAlgorithmPropertyValue(AlgorithmPropertyValue algorithmPropertyValue) {
        algorithmPropertyValues.add(algorithmPropertyValue);
        algorithmPropertyValue.setAlgorithmPropertyDef(this);
    }
}
