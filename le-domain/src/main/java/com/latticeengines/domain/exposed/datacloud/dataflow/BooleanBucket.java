package com.latticeengines.domain.exposed.datacloud.dataflow;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.statistics.BucketType;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class BooleanBucket extends BucketAlgorithm {

    public static final String DEFAULT_TRUE = "Yes";
    public static final String DEFAULT_FALSE = "No";

    @Override
    @JsonIgnore
    public String getAlgorithm() {
        return BOOLEAN;
    }

    @JsonProperty("t")
    private String trueLabel;

    @JsonProperty("f")
    private String falseLabel;

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

    @JsonIgnore
    public String getTrueLabelWithDefault() {
        return StringUtils.isBlank(trueLabel) ? DEFAULT_TRUE : trueLabel;
    }

    @JsonIgnore
    public String getFalseLabelWithDefault() {
        return StringUtils.isBlank(falseLabel) ? DEFAULT_FALSE : falseLabel;
    }

    @Override
    @JsonIgnore
    public List<String> generateLabelsInternal () {
        return Arrays.asList(null, getTrueLabelWithDefault(), getFalseLabelWithDefault());
    }

    @JsonIgnore
    @Override
    public BucketType getBucketType() {
        return BucketType.Boolean;
    }

}
