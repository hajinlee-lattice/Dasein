package com.latticeengines.domain.exposed.propdata;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.pls.ResponseDocument;

@SuppressWarnings("rawtypes")
public class ResponseCommandStatus extends ResponseDocument{
	private String status;
	
	public ResponseCommandStatus(String status) {
		this.status = status;
	}

	@JsonProperty("status")
	public String getStatus() {
		return status;
	}

	@JsonProperty("status")
	public void setStatus(String status) {
		this.status = status;
	}
	
	
}
