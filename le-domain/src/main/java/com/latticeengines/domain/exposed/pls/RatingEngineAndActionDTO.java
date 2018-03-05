package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class RatingEngineAndActionDTO {

    public RatingEngineAndActionDTO() {
    }

    public RatingEngineAndActionDTO(RatingEngine ratingEngine, Action action) {
        this.ratingEngine = ratingEngine;
        this.action = action;
    }

    @JsonProperty("ratingEngine")
    private RatingEngine ratingEngine;

    @JsonProperty("action")
    private Action action;

    public RatingEngine getRatingEngine() {
        return this.ratingEngine;
    }

    public void setRatingEngine(RatingEngine ratingEngine) {
        this.ratingEngine = ratingEngine;
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
