package com.latticeengines.dataplatform.exposed.domain;

public class ModelDefinition implements HasName {
	
	private String name;
	private boolean willSample;
	
	@Override
	public String getName() {
		return name;
	}

	@Override
	public void setName(String name) {
		this.name = name;
	}

	public boolean willSample() {
		return willSample;
	}

	public void setWillSample(boolean willSample) {
		this.willSample = willSample;
	}

}
