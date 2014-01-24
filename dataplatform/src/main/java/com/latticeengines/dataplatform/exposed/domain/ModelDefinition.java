package com.latticeengines.dataplatform.exposed.domain;

public class ModelDefinition implements HasName {
	
	private String name;
	private boolean willSample;
	private ExecutorType executorType;
	private DistributionType distributionType;
	
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

	public ExecutorType getExecutorType() {
		return executorType;
	}

	public void setExecutorType(ExecutorType executorType) {
		this.executorType = executorType;
	}

	public DistributionType getDistributionType() {
		return distributionType;
	}

	public void setDistributionType(DistributionType distributionType) {
		this.distributionType = distributionType;
	}

}
