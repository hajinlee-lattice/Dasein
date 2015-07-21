package com.latticeengines.domain.exposed.propdata;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.pls.ResponseDocument;

@SuppressWarnings("rawtypes")
public class ResponseID extends ResponseDocument{

    @SuppressWarnings("unchecked")
    public ResponseID(Boolean success,List<String> errors
            ,@JsonProperty("ID")  Long ID) {
        this.setSuccess(success);
        this.setErrors(errors);
        this.setResult(ID);
    }        
}
