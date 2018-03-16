package com.latticeengines.apps.cdl.rating;

import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.ModelingConfig;
import com.latticeengines.domain.exposed.pls.ModelingConfigFilter;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.TimeFilter;

public class CrossSellRatingTargetQueryBuilder extends CrossSellRatingQueryBuilder {

    protected CrossSellRatingTargetQueryBuilder(RatingEngine ratingEngine, AIModel aiModel, int evaluationPeriod) {
        super(ratingEngine, aiModel, evaluationPeriod);
    }

    @Override
    protected void handleCustomSegment() {
    }

    @Override
    protected void handleProxyProducts() {
    }

    @Override
    protected void removeTimeWindowRestrictions() {
    }

    @Override
    protected void buildProductTransactionRestrictions() {
        AttributeLookup attrLookup = new AttributeLookup(BusinessEntity.Transaction, productIds);
        Bucket.Transaction txn;
        switch (aiModel.getModelingStrategy()) {

        case CROSS_SELL_REPEAT_PURCHASE:
            ModelingConfigFilter config = aiModel.getModelingConfigFilters()
                    .get(ModelingConfig.PURCHASED_BEFORE_PERIOD);
            if (config == null) {
                throw new LedpException(LedpCode.LEDP_40011, new String[] { aiModel.getId() });
            }
            productTxnRestriction = new BucketRestriction(attrLookup, Bucket.txnBkt(new Bucket.Transaction(productIds,
                    TimeFilter.priorOnly(config.getValue() - 1, TimeFilter.Period.Month), null, null, false)));
            break;
        case CROSS_SELL_FIRST_PURCHASE:
            txn = new Bucket.Transaction(productIds, TimeFilter.ever(), null, null, true);
            productTxnRestriction = new BucketRestriction(attrLookup, Bucket.txnBkt(txn));
            break;
        default:
            throw new LedpException(LedpCode.LEDP_40017);
        }
    }

    @Override
    protected void handleCustomTrainingPeriod() {
        // Do nothing for target query
    }
}
