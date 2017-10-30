package com.latticeengines.domain.exposed.query;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class WindowFunctionLookup extends Lookup {

    @JsonProperty("function_type")
    private FunctionType functionType;

    @JsonProperty("target")
    private Lookup target;

    // right now we only support partition by, no order by, will add more as needed
    @JsonProperty("partition_by")
    private List<Lookup> partitionBy = new ArrayList<>();

    @JsonProperty("alias")
    private String alias;

    public WindowFunctionLookup() {
    }

    private WindowFunctionLookup(FunctionType functionType, Lookup target) {
        this.functionType = functionType;
        this.target = target;
    }

    public static WindowFunctionLookup sum(Lookup target, Lookup partition, String alias) {
        return new WindowFunctionLookup(FunctionType.SUM, target).withPartition(partition).as(alias);
    }

    public static WindowFunctionLookup max(Lookup target, String alias) {
        return new WindowFunctionLookup(FunctionType.MAX, target).as(alias);
    }

    public WindowFunctionLookup withPartition(Lookup partition) {
        this.partitionBy.add(partition);
        return this;
    }

    public WindowFunctionLookup as(String alias) {
        this.alias = alias;
        return this;
    }

    public FunctionType getFunctionType() {
        return functionType;
    }

    public Lookup getTarget() {
        return target;
    }

    public List<Lookup> getPartitionBy() {
        return partitionBy;
    }

    public String getAlias() {
        return alias;
    }

    public void setFunctionType(FunctionType functionType) {
        this.functionType = functionType;
    }

    public void setTarget(Lookup target) {
        this.target = target;
    }

    public void setPartitionBy(List<Lookup> partitionBy) {
        this.partitionBy = partitionBy;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public enum FunctionType {
        SUM, AVG, MAX, MIN
    }
}
