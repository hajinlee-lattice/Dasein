package com.latticeengines.domain.exposed.dataplatform;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Table;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import org.hibernate.annotations.LazyCollection;
import org.hibernate.annotations.LazyCollectionOption;

import com.latticeengines.domain.exposed.dataplatform.algorithm.AlgorithmBase;

@Entity
@Table(name = "MODEL_DEFINITION")
public class ModelDefinition implements HasName, HasPid {

    private Long pid;
    private String name;
    private List<Algorithm> algorithms = new ArrayList<Algorithm>();
    private List<Model> models = new ArrayList<Model>();

    @Override
    @Id
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
    @Column(name = "NAME")
    public String getName() {
        return name;
    }

    @Override
    @JsonProperty("name")
    public void setName(String name) {
        this.name = name;
    }

    @JsonProperty("algorithms")
    @OneToMany(mappedBy = "modelDefinition", targetEntity = AlgorithmBase.class, fetch=FetchType.EAGER )
    public List<Algorithm> getAlgorithms() {
        return algorithms;
    }

    @JsonProperty("algorithms")
    public void setAlgorithms(List<Algorithm> algorithms) {
        this.algorithms = algorithms;
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
