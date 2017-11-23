package com.latticeengines.domain.exposed.pls;

public class ModelingConfigFilter {

	private MODEL_CONFIG configName;
	
	private String criteria;
	
	private Integer value;
	
	public MODEL_CONFIG getConfigName() {
		return configName;
	}

	public void setConfigName(MODEL_CONFIG configName) {
		this.configName = configName;
	}

	public String getCriteria() {
		return criteria;
	}

	public void setCriteria(String criteria) {
		this.criteria = criteria;
	}

	public Integer getValue() {
		return value;
	}

	public void setValue(Integer value) {
		this.value = value;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == null || !(obj instanceof ModelingConfigFilter)) {
			return false;
		}
		ModelingConfigFilter newObj = (ModelingConfigFilter)obj;
		return this.configName != null && this.configName.equals(newObj.configName);
	}	

	@Override
	public int hashCode() {
		return this.configName != null ? this.configName.hashCode() : super.hashCode();
	}


	public static enum MODEL_CONFIG {
		SPEND_IN_PERIOD,
		QUANTITY_IN_PERIOD,
		HISTORICAL_RECORDS
	}
}
