package com.latticeengines.domain.exposed.ratings.coverage;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.latticeengines.common.exposed.util.JsonUtils;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RatingModelIdPair {
    private String ratingEngineId;

    private String ratingModelId;

    public String getRatingEngineId() {
        return ratingEngineId;
    }

    public void setRatingEngineId(String ratingEngineId) {
        this.ratingEngineId = ratingEngineId;
    }

    public String getRatingModelId() {
        return ratingModelId;
    }

    public void setRatingModelId(String ratingModelId) {
        this.ratingModelId = ratingModelId;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
