package com.latticeengines.domain.exposed.spark;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "Type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = InputStreamSparkScript.class, name = "InputStream"),
        @JsonSubTypes.Type(value = LocalFileSparkScript.class, name = "LocalFile")
})
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect( //
        fieldVisibility = JsonAutoDetect.Visibility.NONE, //
        getterVisibility = JsonAutoDetect.Visibility.NONE, //
        isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
        setterVisibility = JsonAutoDetect.Visibility.NONE //
)
public abstract class SparkScript {

    @JsonProperty("Type")
    public abstract Type getType();

    @JsonProperty("Interpreter")
    private SparkInterpreter interpreter;

    public SparkInterpreter getInterpreter() {
        return interpreter;
    }

    public void setInterpreter(SparkInterpreter interpreter) {
        this.interpreter = interpreter;
    }

    public enum Type {
        InputStream, LocalFile
    }

}
