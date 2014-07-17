package com.latticeengines.domain.exposed.dataplatform.algorithm;

import java.util.Properties;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.DiscriminatorColumn;
import javax.persistence.DiscriminatorType;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.Transient;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.StringTokenUtils;
import com.latticeengines.domain.exposed.dataplatform.Algorithm;
import com.latticeengines.domain.exposed.dataplatform.ModelDefinition;

@Entity
@Table(name = "ALGORITHM")
@Inheritance(strategy=InheritanceType.SINGLE_TABLE)
@DiscriminatorColumn(name="ALGO_TYPE", discriminatorType=DiscriminatorType.STRING)
public class AlgorithmBase implements Algorithm {

    private Long pid;
    private String name;
    private String script;
    private String containerProperties;
    private String algorithmProperties;
    private int priority; 
    private String sampleName;
    private ModelDefinition modelDefinition;

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
    @Column(name = "NAME", nullable = false)
    public String getName() {
        return name;
    }

    @Override
    @JsonProperty("name")
    public void setName(String name) {
        this.name = name;
    }

    @JsonIgnore
    @Override
    @ManyToOne
    @JoinColumn(name = "FK_MODEL_DEF_ID")
    public ModelDefinition getModelDefinition() {
        return modelDefinition;
    }

    @Override 
    public void setModelDefinition(ModelDefinition modelDefinition) {
        this.modelDefinition = modelDefinition;
    }

    @Override
    @JsonProperty("script")
    @Column(name = "SCRIPT", length = 500)
    public String getScript() {
        return script;
    }

    @JsonProperty("script")
    public void setScript(String script) {
        this.script = script;
    }

    @Override
    @JsonProperty("container_properties")
    @Column(name = "CONTAINER_PROPERTIES", length = 65535)
    public String getContainerProperties() {
        return containerProperties;
    }

    @Override
    @JsonProperty("container_properties")
    public void setContainerProperties(String containerProperties) {
        this.containerProperties = containerProperties;
    }

    @Override
    @JsonProperty("algorithm_properties")
    @Column(name = "ALGORITHM_PROPERTIES", length = 65535)
    public String getAlgorithmProperties() {
        return algorithmProperties;
    }

    @Override
    @JsonProperty("algorithm_properties")
    public void setAlgorithmProperties(String algorithmProperties) {
        this.algorithmProperties = algorithmProperties;
    }

    @Override
    @JsonProperty("priority")
    @Column(name = "PRIORITY", nullable = false)
    public int getPriority() {
        return priority;
    }

    @Override
    @JsonProperty("priority")
    public void setPriority(int priority) {
        this.priority = priority;
    }

    @Override
    @JsonProperty("sample")
    @Column(name = "SAMPLE_NAME")
    public String getSampleName() {
        return sampleName;
    }

    @Override
    @JsonProperty("sample")
    public void setSampleName(String sampleName) {
        this.sampleName = sampleName;
    }

    @Override
    @JsonIgnore
    @Transient
    public Properties getAlgorithmProps() {
        return StringTokenUtils.stringToProperty(getAlgorithmProperties());
    }

    @Override
    @JsonIgnore
    @Transient
    public Properties getContainerProps() {
        return StringTokenUtils.stringToProperty(getContainerProperties());
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
