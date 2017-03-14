package com.latticeengines.domain.exposed.ulysses;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.swagger.annotations.ApiModelProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_EMPTY)
public class PrimaryFieldConfiguration {
	
    @JsonProperty("primaryFields")
    @ApiModelProperty(required = true, value = "List of Primary Fields")
    private List<PrimaryField> primaryFields;

    @JsonProperty("validationExpression")
    @ApiModelProperty(required = false, value = "Validation Expression wrapper")
    private PrimaryFieldValidationExpression validationExpression;

	public List<PrimaryField> getPrimaryFields() {
		return primaryFields;
	}

	public void setPrimaryFields(List<PrimaryField> primaryFields) {
		this.primaryFields = primaryFields;
	}

	public PrimaryFieldValidationExpression getValidationExpression() {
		return validationExpression;
	}

	public void setValidationExpression(PrimaryFieldValidationExpression validationExpression) {
		this.validationExpression = validationExpression;
	}
    
}
