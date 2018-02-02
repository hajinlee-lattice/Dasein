package com.latticeengines.apps.cdl.rating;

import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.RatingEngine;

public class CrossSellRatingTargetQueryBuilder extends CrossSellRatingQueryBuilder {

    protected CrossSellRatingTargetQueryBuilder(RatingEngine ratingEngine, AIModel aiModel) {
        super(ratingEngine, aiModel);
    }

    @Override
    protected void handleCustomSegment() {
        // Do Nothing
    }

    @Override
    protected void handleProxyProducts() {
        // Do Nothing
    }

    @Override
    protected void buildProductTransactionRestrictions() {
        // Do Nothing
    }

    @Override
    protected void handleCustomTrainingPeriod() {
        // Do nothing for target query
    }
}
