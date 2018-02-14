package com.latticeengines.apps.cdl.rating;

import org.apache.commons.collections4.CollectionUtils;

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

public class CrossSellRatingTrainingQueryBuilder extends CrossSellRatingQueryBuilder {

    protected CrossSellRatingTrainingQueryBuilder(RatingEngine ratingEngine, AIModel aiModel) {
        super(ratingEngine, aiModel);
    }

    @Override
    protected void handleCustomSegment() {
        if (aiModel.getTrainingSegment() != null) {
            baseSegment = aiModel.getTrainingSegment();
        }
    }

    @Override
    protected void handleProxyProducts() {
        if (CollectionUtils.isEmpty(aiModel.getTrainingProducts()))
            return;
        productIds = String.join(",", aiModel.getTrainingProducts());
    }

    @Override
    protected void handleCustomTrainingPeriod() {
        ModelingConfigFilter filter = aiModel.getModelingConfigFilters().get(ModelingConfig.TRAINING_SET_PERIOD);
        if (filter != null) {
            ratingFrontEndQuery.setPeriodCount(filter.getValue());
        }
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
            txn = new Bucket.Transaction(productIds, priorOnly(config.getValue()), null, null, false);
        } else {
            txn = new Bucket.Transaction(productIds, TimeFilter.ever(), null, null, true);
        }
        Bucket txnBucket = Bucket.txnBkt(txn);
        productTxnRestriction = new BucketRestriction(attrLookup, txnBucket);
    }
}