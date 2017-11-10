package com.latticeengines.pls.proxy;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.testframework.exposed.proxy.pls.PlsRestApiProxyBase;

@Component("testRatingEngineProxy")
public class TestRatingEngineProxy extends PlsRestApiProxyBase {

    public TestRatingEngineProxy() {
        super("pls/ratingengines");
    }

    public RatingEngine createOrUpdate(RatingEngine ratingEngine) {
        String url = constructUrl("/");
        return post("create rating engine", url, ratingEngine, RatingEngine.class);
    }

    public RatingEngine getRatingEngine(String ratingEngineId) {
        String url = constructUrl("/{ratingEngineId}", ratingEngineId);
        return get("get rating engine", url, RatingEngine.class);
    }

    public RatingModel updateRatingModel(String ratingEngineId, String ratingModelId, RatingModel ratingModel) {
        String url = constructUrl("/{ratingEngineId}/ratingmodels/{ratingModelId}", ratingEngineId, ratingModelId);
        return post("update rating model", url, ratingModel, RatingModel.class);
    }

}
