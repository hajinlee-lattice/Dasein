package com.latticeengines.cloudmodel;

import java.util.ArrayList;

public class BaseCloudResult {
	public BaseCloudUpdate update = null;
	public boolean isSuccess = false;
	public String requestId = null;
	public ArrayList<String> jsonObjectResults = null;
	public String errorMessage = null;
	
	public BaseCloudResult(
			BaseCloudUpdate update,
			boolean isSuccess,
			String requestId,
			ArrayList<String> jsonObjectResults) {
		this.update = update;
		this.isSuccess = isSuccess;
		this.requestId = requestId;
		this.jsonObjectResults = jsonObjectResults;
	}

	public BaseCloudResult(
			BaseCloudUpdate update,
			boolean isSuccess,
			String requestId,
			String errorMessage) {
		this.update = update;
		this.isSuccess = isSuccess;
		this.requestId = requestId;
		this.errorMessage = errorMessage;
	}
}
