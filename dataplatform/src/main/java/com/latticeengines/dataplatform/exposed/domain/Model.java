package com.latticeengines.dataplatform.exposed.domain;

public class Model implements HasName {

	private String name;
	private String sourceDataPath;
	private ModelDefinition modelDefinition;
	
	@Override
	public String getName() {
		return name;
	}

	@Override
	public void setName(String name) {
		this.name = name;
	}

	public String getSourceDataPath() {
		return sourceDataPath;
	}

	public void setSourceDataPath(String sourceDataPath) {
		this.sourceDataPath = sourceDataPath;
	}

	public ModelDefinition getModelDefinition() {
		return modelDefinition;
	}

	public void setModelDefinition(ModelDefinition modelDefinition) {
		this.modelDefinition = modelDefinition;
	}

}
