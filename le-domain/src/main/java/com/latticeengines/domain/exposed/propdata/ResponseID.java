package com.latticeengines.domain.exposed.propdata;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.pls.ResponseDocument;

@SuppressWarnings("rawtypes")
public class ResponseID extends ResponseDocument{
	private Long ID;
	
	public ResponseID(Long iD) {
		this.ID = iD;
	}

	@JsonProperty("ID")
	public Long getID() {
		return ID;
	}

	@JsonProperty("ID")
	public void setID(Long iD) {
		ID = iD;
	}
	
	
}
