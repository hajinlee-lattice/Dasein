package com.latticeengines.domain.exposed.admin;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class DerivedField {

    private String expression;
    private List<Parameter> parameters;

    @JsonProperty("Expression")
    public String getExpression() { return expression; }
    @JsonProperty("Expression")
    public void setExpression(String expression) { this.expression = expression; }
    @JsonProperty("Parameters")
    public List<Parameter> getParameters() { return parameters; }
    @JsonProperty("Parameters")
    public void setParameters(List<Parameter> parameters) { this.parameters = parameters; }

    private static class Parameter {
        public String component;
        public String nodePath;

        @JsonProperty("Component")
        public String getComponent() { return component; }
        @JsonProperty("Component")
        public void setComponent(String component) { this.component = component; }
        @JsonProperty("NodePath")
        public String getNodePath() { return nodePath; }
        @JsonProperty("NodePath")
        public void setNodePath(String nodePath) { this.nodePath = nodePath; }
    }
}
