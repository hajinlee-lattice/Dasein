package com.latticeengines.domain.exposed.modeling.algorithm;

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
import javax.persistence.Lob;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.Transient;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.StringTokenUtils;
import com.latticeengines.domain.exposed.modeling.Algorithm;
import com.latticeengines.domain.exposed.modeling.ModelDefinition;

@Entity
@Table(name = "ALGORITHM")
@Inheritance(strategy = InheritanceType.SINGLE_TABLE)
@DiscriminatorColumn(name = "ALGO_TYPE", discriminatorType = DiscriminatorType.STRING)
public class AlgorithmBase implements Algorithm {

    private Long pid;
    private String name;
    private String script;
    private String containerProperties;
    private String algorithmProperties;
    private int priority;
    private String sampleName;
    private ModelDefinition modelDefinition;
    private String pipelineScript;
    private String pipelineLibScript;
    private String mapperSize = "1";
    private String pipelineProperties;
    private String pipelineDriver;

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

    @JsonIgnore
    @Override
    @ManyToOne
    @JoinColumn(name = "FK_MODEL_DEF_ID")
    public ModelDefinition getModelDefinition() {
        return modelDefinition;
    }

    @JsonIgnore
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
    @Column(name = "CONTAINER_PROPERTIES")
    @Lob
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
    @Column(name = "ALGORITHM_PROPERTIES")
    @Lob
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

    @JsonProperty("pipeline_script")
    @Column(name = "PIPELINE_SCRIPT", nullable = true)
    public String getPipelineScript() {
        return pipelineScript;
    }

    @JsonProperty("pipeline_script")
    public void setPipelineScript(String pipelineScript) {
        this.pipelineScript = pipelineScript;
    }

    @JsonProperty("pipeline_lib_script")
    @Column(name = "PIPELINE_LIB_SCRIPT", nullable = true)
    public String getPipelineLibScript() {
        return pipelineLibScript;
    }

    @JsonProperty("pipeline_lib_script")
    public void setPipelineLibScript(String pipelineLibScript) {
        this.pipelineLibScript = pipelineLibScript;
    }

    @Transient
    @JsonIgnore
    public String getMapperSize() {
        return this.mapperSize;
    }
    
    @JsonIgnore
    public void setMapperSize(String mapperSize) {
        this.mapperSize = mapperSize;
    }

    @Override
    public void setPipelineProperties(String pipelineProperties) {
        this.pipelineProperties = pipelineProperties;
    }

    @Override
    @JsonProperty("pipeline_properties")
    @Column(name = "PIPELINE_PROPERTIES", nullable = true)
    @Lob
    public String getPipelineProperties() {
        return pipelineProperties;
    }

    @Override
    @JsonIgnore
    @Transient
    public Properties getPipelineProps() {
        return StringTokenUtils.stringToProperty(getPipelineProperties());
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    @Override
    @JsonProperty("pipeline_driver")
    public void setPipelineDriver(String pipelineDriver) {
        this.pipelineDriver = pipelineDriver;
    }
    
    @Override
    @JsonProperty("pipeline_driver")
    @Column(name = "PIPELINE_DRIVER", nullable = true)
    public String getPipelineDriver() {
        return pipelineDriver;
    }

    @Override
	public void resetAlgorithmProperties() {
	}


}
