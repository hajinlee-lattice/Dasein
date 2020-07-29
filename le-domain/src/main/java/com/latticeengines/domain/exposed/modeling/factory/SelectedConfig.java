package com.latticeengines.domain.exposed.modeling.factory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.annotation.MetricTagGroup;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.modelquality.Algorithm;
import com.latticeengines.domain.exposed.modelquality.Pipeline;
import com.latticeengines.domain.exposed.modelquality.Sampling;

/**
 * This class represents the holder for all the top-level dimensions. This class
 * would be passed to the workflow that would kick off either the file-based
 * self-service modeling or the event table model workflow.
 *
 * @author rgonzalez
 *
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect( //
        fieldVisibility = JsonAutoDetect.Visibility.NONE, //
        getterVisibility = JsonAutoDetect.Visibility.NONE, //
        isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
        setterVisibility = JsonAutoDetect.Visibility.NONE //
)
public class SelectedConfig implements Fact, Dimension {

    @JsonProperty("pipeline")
    private Pipeline pipeline;

    @JsonProperty("algorithm")
    private Algorithm algorithm;

    @JsonProperty("sampling")
    private Sampling sampling;

    @MetricTagGroup
    public Pipeline getPipeline() {
        return pipeline;
    }

    public void setPipeline(Pipeline pipeline) {
        this.pipeline = pipeline;
    }

    @MetricTagGroup
    public Algorithm getAlgorithm() {
        return algorithm;
    }

    public void setAlgorithm(Algorithm algorithm) {
        this.algorithm = algorithm;
    }

    @MetricTagGroup
    public Sampling getSampling() {
        return sampling;
    }

    public void setSampling(Sampling sampling) {
        this.sampling = sampling;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
}
