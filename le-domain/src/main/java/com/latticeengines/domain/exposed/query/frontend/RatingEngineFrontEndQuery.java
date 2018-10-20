package com.latticeengines.domain.exposed.query.frontend;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class RatingEngineFrontEndQuery extends FrontEndQuery {

    @JsonProperty(FrontEndQueryConstants.RATING_ENGINE_ID)
    private String ratingEngineId;

    public static RatingEngineFrontEndQuery fromFrontEndQuery(FrontEndQuery frontEndQuery) {
        return JsonUtils.deserialize(JsonUtils.serialize(frontEndQuery),
                RatingEngineFrontEndQuery.class);
    }

    public String getRatingEngineId() {
        return ratingEngineId;
    }

    public void setRatingEngineId(String ratingEngineId) {
        this.ratingEngineId = ratingEngineId;
    }

}
