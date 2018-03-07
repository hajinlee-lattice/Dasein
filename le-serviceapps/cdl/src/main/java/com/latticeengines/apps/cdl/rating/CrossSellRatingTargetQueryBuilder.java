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
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.TimeFilter;

public class CrossSellRatingTargetQueryBuilder extends CrossSellRatingQueryBuilder {

    protected CrossSellRatingTargetQueryBuilder(RatingEngine ratingEngine, AIModel aiModel) {
        super(ratingEngine, aiModel);
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
        if (aiModel.getModelingStrategy() == ModelingStrategy.CROSS_SELL_REPEAT_PURCHASE) {
            ModelingConfigFilter config = aiModel.getModelingConfigFilters()
                    .get(ModelingConfig.PURCHASED_BEFORE_PERIOD);
            if (config == null) {
                throw new LedpException(LedpCode.LEDP_40011, new String[] { aiModel.getId() });
            }
            BucketRestriction priorOnly = new BucketRestriction(attrLookup,
                    Bucket.txnBkt(new Bucket.Transaction(productIds,
                            TimeFilter.priorOnly(config.getValue(), TimeFilter.Period.Month), null, null, false)));

            BucketRestriction notWithin = new BucketRestriction(attrLookup,
                    Bucket.txnBkt(new Bucket.Transaction(productIds,
                            TimeFilter.within(config.getValue(), TimeFilter.Period.Month), null, null, true)));
            productTxnRestriction = Restriction.builder().and(priorOnly, notWithin).build();
        } else {
            txn = new Bucket.Transaction(productIds, TimeFilter.ever(), null, null, true);
            productTxnRestriction = new BucketRestriction(attrLookup, Bucket.txnBkt(txn));
        }
    }

    @Override
    protected void handleCustomTrainingPeriod() {
        // Do nothing for target query
    }
}
