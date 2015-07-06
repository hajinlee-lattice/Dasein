package com.latticeengines.domain.exposed.propdata;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.pls.ResponseDocument;

@SuppressWarnings("rawtypes")
public class ResponseCommandStatus extends ResponseDocument{
	
	@SuppressWarnings("unchecked")
	public ResponseCommandStatus(Boolean success, List<String> errors
			,@JsonProperty("status")   String status) {
		this.setSuccess(success);
        this.setErrors(errors);
		this.setResult(status);
	}	
}
