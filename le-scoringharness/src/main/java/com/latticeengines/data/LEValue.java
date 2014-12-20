package com.latticeengines.data;

// TODO: Support other primitive types
public class LEValue {
	private String rawValue = null;

	public LEValue() {
	}
	
	public LEValue(String value) {
		this.rawValue = value;
	}
	
	public void setValue(String value) {
		rawValue = value;
	}
	
	public String getValueString() {
		return rawValue;
	}
}
