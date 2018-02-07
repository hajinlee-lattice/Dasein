package com.latticeengines.apps.cdl.rating;

import com.latticeengines.domain.exposed.cdl.ModelingStrategy;
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
    protected void removeTimeWindowRestrictions() {
        // Do Nothing
    }

    @Override
    protected void buildProductTransactionRestrictions() {
        AttributeLookup attrLookup = new AttributeLookup(BusinessEntity.Transaction, productIds); // Doesn't
        // matter
        Bucket.Transaction txn;
        if (aiModel.getModelingStrategy() == ModelingStrategy.CROSS_SELL_REPEAT_PURCHASE) {
            ModelingConfigFilter config = aiModel.getModelingConfigFilters()
                    .get(ModelingConfig.PURCHASED_BEFORE_PERIOD);
            if (config == null) {
                throw new LedpException(LedpCode.LEDP_40011, new String[] { aiModel.getId() });
            }
            txn = new Bucket.Transaction(productIds, priorOnly(config.getValue()), null, null, false);
        } else {
            txn = new Bucket.Transaction(productIds, TimeFilter.ever(), null, null, true);
        }
        Bucket txnBucket = Bucket.txnBkt(txn);
        productTxnRestriction = new BucketRestriction(attrLookup, txnBucket);
    }

    @Override
    protected void handleCustomTrainingPeriod() {
        // Do nothing for target query
    }
}
