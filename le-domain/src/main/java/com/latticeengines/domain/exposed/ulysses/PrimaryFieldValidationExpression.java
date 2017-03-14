package com.latticeengines.domain.exposed.ulysses;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import io.swagger.annotations.ApiModelProperty;

/**
 * @author jadusumalli Validation Expression wrapper
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_EMPTY)
public class PrimaryFieldValidationExpression {
	/*
	 * Created as a complex object, with the expectation of supporting object specific validations
	 * Ex: Account Object, Lead Object and Contact Object can have different 
	 * validation requirements with more fine grain requirements.
	 */
	@JsonProperty("expression")
	@ApiModelProperty(required = true, value = "Default validation expression for Primary fields")
	private String expression;

	public String getExpression() {
		return expression;
	}

	public void setExpression(String expression) {
		this.expression = expression;
	}

}
