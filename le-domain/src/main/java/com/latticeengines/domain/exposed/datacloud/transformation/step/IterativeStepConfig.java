package com.latticeengines.domain.exposed.datacloud.transformation.step;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
@JsonSubTypes({ @JsonSubTypes.Type(value = IterativeStepConfig.ConvergeOnCount.class, name = "ConvergeOnCount"), })
public class IterativeStepConfig {

    public static final String ITERATE_STRATEGY = "IterateStrategy";

    @JsonProperty("IteratingSource")
    private String iteratingSource;

    public String getIteratingSource() {
        return iteratingSource;
    }

    public void setIteratingSource(String iteratingSource) {
        this.iteratingSource = iteratingSource;
    }

    public static class ConvergeOnCount extends IterativeStepConfig {
        @JsonProperty("CountDiff")
        private int countDiff;

        public int getCountDiff() {
            return countDiff;
        }

        public void setCountDiff(int countDiff) {
            this.countDiff = countDiff;
        }
    }

}
