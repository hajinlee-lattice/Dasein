package com.latticeengines.dataplatform.exposed.domain;

import java.util.Properties;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

import com.latticeengines.dataplatform.util.JsonHelper;

public class Algorithm implements HasName {

	private String name;
	private String script;
	private String containerProperties;
	private String algorithmProperties;
	private int priority;
	
	@Override
	@JsonProperty("name")
	public String getName() {
		return name;
	}

	@Override
	@JsonProperty("name")
	public void setName(String name) {
		this.name = name;
	}

	@JsonProperty("script")
	public String getScript() {
		return script;
	}

	@JsonProperty("script")
	public void setScript(String script) {
		this.script = script;
	}

	@JsonProperty("container_properties")
	public String getContainerProperties() {
		return containerProperties;
	}

	@JsonProperty("container_properties")
	public void setContainerProperties(String containerProperties) {
		this.containerProperties = containerProperties;
	}

	@JsonProperty("algorithm_properties")
	public String getAlgorithmProperties() {
		return algorithmProperties;
	}

	@JsonProperty("algorithm_properties")
	public void setAlgorithmProperties(String algorithmProperties) {
		this.algorithmProperties = algorithmProperties;
	}
	
	@JsonIgnore
	public Properties getAlgorithmProps() {
		return createPropertiesFromString(getAlgorithmProperties());
	}
	
	@JsonIgnore
	public Properties getContainerProps() {
		return createPropertiesFromString(getContainerProperties());
	}
	
	private static Properties createPropertiesFromString(String value) {
		Properties props = new Properties();
		String[] propertyKeyValues = value.split(" ");
		for (String propertyKeyValue : propertyKeyValues) {
			String[] kv = propertyKeyValue.split("=");
			props.setProperty(kv[0], kv[1]);
		}
		return props;
	}
	
	@Override
	public String toString() {
		return JsonHelper.serialize(this);
	}

	@JsonProperty("priority")
	public int getPriority() {
		return priority;
	}

	@JsonProperty("priority")
	public void setPriority(int priority) {
		this.priority = priority;
	}

}
