package com.latticeengines.domain.exposed.dataplatform.algorithm;

import java.util.Properties;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.Transient;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.Algorithm;
import com.latticeengines.domain.exposed.dataplatform.ModelDefinition;

@Entity
@Table(name = "ALGORITHM")
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
	@Column(name = "NAME")
	public String getName() {
		return name;
	}

	@Override
	@JsonProperty("name")
	public void setName(String name) {
		this.name = name;
	}

	@ManyToOne
	@JoinColumn(name = "FK_MODEL_DEF_ID")
	public ModelDefinition getModelDefinition() {
		return modelDefinition;
	}

	public void setModelDefinition(ModelDefinition modelDefinition) {
		this.modelDefinition = modelDefinition;
	}

	@Override
	@JsonProperty("script")
	@Column(name = "SCRIPT")
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
	@Column(name = "PRIORITY")
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
		return createPropertiesFromString(getAlgorithmProperties());
	}

	@Override
	@JsonIgnore
	@Transient
	public Properties getContainerProps() {
		return createPropertiesFromString(getContainerProperties());
	}

	private static Properties createPropertiesFromString(String value) {
		Properties props = new Properties();
		if (value == null) {
			return props;
		}
		String[] propertyKeyValues = value.split(" ");
		for (String propertyKeyValue : propertyKeyValues) {
			String[] kv = propertyKeyValue.split("=");
			props.setProperty(kv[0], kv[1]);
		}
		return props;
	}

	@Override
	public String toString() {
		return JsonUtils.serialize(this);
	}



}
