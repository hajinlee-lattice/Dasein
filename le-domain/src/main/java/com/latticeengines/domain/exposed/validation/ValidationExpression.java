package com.latticeengines.domain.exposed.validation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.swagger.annotations.ApiModelProperty;

/**
 * @author jdraadusumalli
 * 
 *         Supports complex representaion of validation expression using JSON
 *         Expression can contain list of fields along with Condition And it can
 *         contain other nested expressions along with other fields
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_EMPTY)
public class ValidationExpression {

    @JsonProperty("expressions")
    @ApiModelProperty(required = false, value = "List of expressions")
    private List<ValidationExpression> expressions;

    @JsonProperty("fields")
    @ApiModelProperty(required = false, value = "List of fields")
    private List<ValidationField> fields;

    @JsonProperty("condition")
    @ApiModelProperty(required = true, value = "Condition")
    private Condition condition;

    public ValidationExpression() {
        super();
    }

    public ValidationExpression(Condition condition, ValidationField... validationFields) {
        this.condition = condition;
        fields = Arrays.asList(validationFields);
    }

    public ValidationExpression(Condition condition, ValidationExpression... validationExpressions) {
        this.condition = condition;
        expressions = Arrays.asList(validationExpressions);
    }

    public ValidationExpression(Condition condition, ValidationExpression validationExpression,
            ValidationField... validationFields) {
        this(condition, validationFields);
        expressions = new ArrayList<>();
        expressions.add(validationExpression);
    }

    public ValidationExpression(Condition condition, List<ValidationExpression> validationExpressions,
            ValidationField... validationFields) {
        this(condition, validationFields);
        expressions = validationExpressions;
    }

    public Condition getCondition() {
        return condition;
    }

    public List<ValidationExpression> getExpressions() {
        return expressions;
    }

    public List<ValidationField> getFields() {
        return fields;
    }

}
