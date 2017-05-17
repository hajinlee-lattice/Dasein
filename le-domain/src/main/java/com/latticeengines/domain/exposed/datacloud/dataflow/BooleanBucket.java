package com.latticeengines.domain.exposed.datacloud.dataflow;

import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class BooleanBucket extends BucketAlgorithm {

    @Override
    @JsonIgnore
    public String getAlgorithm() {
        return BOOLEAN;
    }

    @JsonProperty("true_label")
    private String trueLabel = "Yes";

    @JsonProperty("false_label")
    private String falseLabel = "No";

    public String getTrueLabel() {
        return trueLabel;
    }

    public void setTrueLabel(String trueLabel) {
        this.trueLabel = trueLabel;
    }

    public String getFalseLabel() {
        return falseLabel;
    }

    public void setFalseLabel(String falseLabel) {
        this.falseLabel = falseLabel;
    }

    @Override
    public List<String> generateLabelsInternal () {
        return Arrays.asList(null, getTrueLabel(), getFalseLabel());
    }
}
