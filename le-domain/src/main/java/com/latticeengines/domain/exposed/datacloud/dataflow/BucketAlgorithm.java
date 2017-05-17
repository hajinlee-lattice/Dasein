package com.latticeengines.domain.exposed.datacloud.dataflow;

import static com.latticeengines.domain.exposed.datacloud.dataflow.BucketAlgorithm.BOOLEAN;
import static com.latticeengines.domain.exposed.datacloud.dataflow.BucketAlgorithm.CATEGORICAL;
import static com.latticeengines.domain.exposed.datacloud.dataflow.BucketAlgorithm.INTEVAL;

import java.io.Serializable;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "algorithm")
@JsonSubTypes({
        @JsonSubTypes.Type(value = CategoricalBucket.class, name = CATEGORICAL),
        @JsonSubTypes.Type(value = IntervalBucket.class, name = INTEVAL),
        @JsonSubTypes.Type(value = BooleanBucket.class, name = BOOLEAN)
})
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class BucketAlgorithm implements Serializable {

    public static final String CATEGORICAL = "Categorical";
    public static final String INTEVAL = "Interval";
    public static final String BOOLEAN = "Boolean";

    private List<String> generatedLabels;

    @JsonProperty("algorithm")
    public abstract String getAlgorithm();

    @JsonIgnore
    public List<String> generateLabels() {
        if (generatedLabels == null) {
            generatedLabels = generateLabelsInternal();
        }
        return generatedLabels;
    }

    @JsonIgnore
    public abstract List<String> generateLabelsInternal();

}
