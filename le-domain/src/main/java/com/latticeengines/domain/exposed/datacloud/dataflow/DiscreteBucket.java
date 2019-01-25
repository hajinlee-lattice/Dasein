package com.latticeengines.domain.exposed.datacloud.dataflow;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.statistics.BucketType;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DiscreteBucket extends BucketAlgorithm {
    private static final long serialVersionUID = -4196020293536783489L;
    @JsonProperty("values")
    private List<Number> values;

    @Override
    @JsonIgnore
    public String getAlgorithm() {
        return DISCRETE;
    }

    public List<Number> getValues() {
        return values;
    }

    public void setValues(List<Number> values) {
        this.values = values;
    }

    @JsonIgnore
    @Override
    public List<String> generateLabelsInternal() {
        List<String> labels = new ArrayList<>();
        labels.add(null);
        if (CollectionUtils.isNotEmpty(values)) {
            values.forEach(v -> labels.add(String.valueOf(v)));
        }
        return labels;
    }

    @JsonIgnore
    @Override
    public BucketType getBucketType() {
        return BucketType.Enum;
    }

}
