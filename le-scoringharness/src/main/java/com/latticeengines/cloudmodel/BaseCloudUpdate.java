package com.latticeengines.cloudmodel;

import java.util.ArrayList;

public class BaseCloudUpdate {

	public String objectType = null;
	public String action = null;
	public ArrayList<String> jsonObjects = null;

	public BaseCloudUpdate(
			String objectType,
			String action) {
		this.objectType = objectType;
		this.action = action;
		this.jsonObjects = new ArrayList<String>();
	}
	
	public BaseCloudUpdate(
			String objectType,
			String action,
			ArrayList<String> jsonObjects) {
		this.objectType = objectType;
		this.action = action;
		this.jsonObjects = jsonObjects;
	}
	
	public void addRow(String jsonRow) {
		jsonObjects.add(jsonRow);
	}
}
