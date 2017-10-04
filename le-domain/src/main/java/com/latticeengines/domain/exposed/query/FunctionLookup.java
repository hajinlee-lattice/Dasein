package com.latticeengines.domain.exposed.query;

import java.util.function.Function;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class FunctionLookup<T, I> extends Lookup {

    @JsonProperty("lookup")
    private Lookup lookup;

    @JsonProperty("alias")
    private String alias;

    private Function<I, String> function;

    private I args;

    private Class<T> type;

    public FunctionLookup(Class<T> type, Lookup lookup) {
        this.type = type;
        this.lookup = lookup;
    }

    public Lookup getLookup() {
        return lookup;
    }

    public void setLookup(Lookup lookup) {
        this.lookup = lookup;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public FunctionLookup<T, I> as(String alias) {
        setAlias(alias);
        return this;
    }

    public Class<T> getType() {
        return type;
    }

    public Function<I, String> getFunction() {
        return function;
    }

    public void setFunction(Function<I, String> function) {
        this.function = function;
    }

    public I getAgrs() {
        return args;
    }

    public void setArgs(I args) {
        this.args = args;
    }

}
