package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class RatingModelAndActionDTO {

    @JsonProperty("ratingModel")
    private RatingModel ratingModel;

    @JsonProperty("action")
    private Action action;

    public RatingModelAndActionDTO() {
    }

    public RatingModelAndActionDTO(RatingModel ratingModel, Action action) {
        this.ratingModel = ratingModel;
        this.action = action;
    }

    public RatingModel getRatingModel() {
        return this.ratingModel;
    }

    public void setRatingModel(RatingModel ratingModel) {
        this.ratingModel = ratingModel;
    }

    public Action getAction() {
        return this.action;
    }

    public void setAction(Action action) {
        this.action = action;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
