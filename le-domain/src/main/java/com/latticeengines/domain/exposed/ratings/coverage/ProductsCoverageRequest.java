package com.latticeengines.domain.exposed.ratings.coverage;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.latticeengines.domain.exposed.pls.RatingEngine;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ProductsCoverageRequest {

    private RatingEngine ratingEngine;

    private List<String> productIds;

    public RatingEngine getRatingEngine() {
        return ratingEngine;
    }

    public void setRatingEngine(RatingEngine ratingEngine) {
        this.ratingEngine = ratingEngine;
    }

    public List<String> getProductIds() {
        return productIds;
    }

    public void setProductIds(List<String> productIds) {
        this.productIds = productIds;
    }

}
