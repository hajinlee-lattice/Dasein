package com.latticeengines.domain.exposed.pls.frontend;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ValidateFieldDefinitionsRequest extends FieldDefinitionBody {

}
