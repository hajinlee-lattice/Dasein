package com.latticeengines.domain.exposed.modeling;

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

import org.hibernate.annotations.LazyCollection;
import org.hibernate.annotations.LazyCollectionOption;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.modeling.algorithm.AlgorithmBase;

@Entity
@Table(name = "MODEL_DEFINITION")
public class ModelDefinition implements HasName, HasPid {

    private Long pid;
    private String name;
    private List<Algorithm> algorithms = new ArrayList<Algorithm>();
    private List<Model> models = new ArrayList<Model>();

    @Override
    @Id
    @JsonIgnore
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    public Long getPid() {
        return this.pid;
    }

    @Override
    public void setPid(Long id) {
        this.pid = id;
    }

    @Override
    @JsonProperty("name")
    @Column(name = "NAME", nullable = false)
    public String getName() {
        return name;
    }

    @Override
    @JsonProperty("name")
    public void setName(String name) {
        this.name = name;
    }

    @JsonProperty("algorithms")
    @OneToMany(mappedBy = "modelDefinition", targetEntity = AlgorithmBase.class, fetch = FetchType.EAGER, cascade = { CascadeType.ALL })
    public List<Algorithm> getAlgorithms() {
        return algorithms;
    }

    @JsonIgnore
    public void setAlgorithms(List<Algorithm> algos) {
        this.algorithms = algos;
    }

    /**
     * add algorithm(s) to the model definition - not intended to be used by ORM
     *
     * @param algos
     */
    @JsonProperty("algorithms")
    public void addAlgorithms(List<Algorithm> algos) {
        this.algorithms = (this.algorithms == null) ? new ArrayList<Algorithm>() : this.algorithms;
        if (algos != null && !algos.isEmpty()) {
            this.algorithms.addAll(algos);
            for (Algorithm a : algos) {
                a.setModelDefinition(this);
            }
        }
    }

    /**
     * add algorithm to the model definition
     *
     * @param algos
     */
    public void addAlgorithm(Algorithm algo) {
        algo.setModelDefinition(this);
        this.algorithms.add(algo);
    }

    @JsonIgnore
    @OneToMany(mappedBy = "modelDefinition", fetch = FetchType.LAZY)
    @LazyCollection(LazyCollectionOption.TRUE)
    public List<Model> getModels() {
        return models;
    }

    public void setModels(List<Model> models) {
        this.models = models;
    }

    public void addModel(Model model) {
        this.models.add(model);
    }

}
