package com.latticeengines.dataplatform.exposed.domain.algorithm;

import java.util.Properties;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

import com.latticeengines.dataplatform.exposed.domain.Algorithm;
import com.latticeengines.dataplatform.util.JsonHelper;

public class AlgorithmBase implements Algorithm {
    private String name;
    private String script;
    private String containerProperties;
    private String algorithmProperties;
    private int priority;
    private String sampleName;

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

    @Override
    @JsonProperty("script")
    public String getScript() {
        return script;
    }

    @JsonProperty("script")
    public void setScript(String script) {
        this.script = script;
    }

    @Override
    @JsonProperty("container_properties")
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
    public String getAlgorithmProperties() {
        return algorithmProperties;
    }

    @Override
    @JsonProperty("algorithm_properties")
    public void setAlgorithmProperties(String algorithmProperties) {
        this.algorithmProperties = algorithmProperties;
    }

    @Override
    @JsonIgnore
    public Properties getAlgorithmProps() {
        return createPropertiesFromString(getAlgorithmProperties());
    }

    @Override
    @JsonIgnore
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
        return JsonHelper.serialize(this);
    }

    @Override
    @JsonProperty("priority")
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
	public String getSampleName() {
		return sampleName;
	}

    @Override
    @JsonProperty("sample")
	public void setSampleName(String sampleName) {
		this.sampleName = sampleName;
	}

}
