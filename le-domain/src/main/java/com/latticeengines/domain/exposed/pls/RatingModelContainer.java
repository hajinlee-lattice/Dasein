package com.latticeengines.domain.exposed.pls;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class RatingModelContainer {

    @JsonProperty("model")
    private RatingModel model;

    @JsonProperty("engine")
    private RatingEngineSummary engineSummary;

    private RatingModelContainer() {
    }

    public RatingModelContainer(RatingModel ratingModel) {
        this.model = ratingModel;
    }

    public RatingModelContainer(RatingModel ratingModel, RatingEngineSummary engineSummary) {
        this.model = ratingModel;
        this.engineSummary = engineSummary;
    }

    public RatingModel getModel() {
        return model;
    }

    public void setModel(RatingModel model) {
        this.model = model;
    }

    public RatingEngineSummary getEngineSummary() {
        return engineSummary;
    }

    public void setEngineSummary(RatingEngineSummary engineSummary) {
        this.engineSummary = engineSummary;
    }
}
