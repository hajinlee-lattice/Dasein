package com.latticeengines.apps.cdl.rating;

import java.util.Collections;

import org.apache.commons.collections4.CollectionUtils;

import com.latticeengines.domain.exposed.cdl.ModelingStrategy;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.ModelingConfig;
import com.latticeengines.domain.exposed.pls.ModelingConfigFilter;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.query.AggregationFilter;
import com.latticeengines.domain.exposed.query.AggregationSelector;
import com.latticeengines.domain.exposed.query.AggregationType;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.TimeFilter;

public class CrossSellRatingEventQueryBuilder extends CrossSellRatingQueryBuilder {

    protected CrossSellRatingEventQueryBuilder(RatingEngine ratingEngine, AIModel aiModel) {
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
        if (!CollectionUtils.isEmpty(aiModel.getTrainingProducts())) {
            productIds = String.join(",", aiModel.getTrainingProducts());
        }
    }

    @Override
    protected void removeTimeWindowRestrictions() {
        // to be implemented
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
        AggregationFilter unitsFilter = null;
        AggregationFilter spentFilter = null;

        if (aiModel.getModelingConfigFilters().containsKey(ModelingConfig.QUANTITY_IN_PERIOD)) {
            ModelingConfigFilter configFilter = aiModel.getModelingConfigFilters()
                    .get(ModelingConfig.QUANTITY_IN_PERIOD);
            unitsFilter = new AggregationFilter(AggregationSelector.UNIT, AggregationType.AT_LEAST_ONCE,
                    configFilter.getCriteria(), Collections.singletonList(configFilter.getValue()));
        }

        if (aiModel.getModelingConfigFilters().containsKey(ModelingConfig.SPEND_IN_PERIOD)) {
            ModelingConfigFilter configFilter = aiModel.getModelingConfigFilters().get(ModelingConfig.SPEND_IN_PERIOD);
            spentFilter = new AggregationFilter(AggregationSelector.SPENT, AggregationType.AT_LEAST_ONCE,
                    configFilter.getCriteria(), Collections.singletonList(configFilter.getValue()));
        }

        Bucket.Transaction txn;
        if (aiModel.getModelingStrategy() == ModelingStrategy.CROSS_SELL_REPEAT_PURCHASE) {
            if (aiModel.getModelingConfigFilters().containsKey(ModelingConfig.PURCHASED_BEFORE_PERIOD)) {
                ModelingConfigFilter config = aiModel.getModelingConfigFilters()
                        .get(ModelingConfig.PURCHASED_BEFORE_PERIOD);
                txn = new Bucket.Transaction(productIds, priorOnly(config.getValue()), spentFilter, unitsFilter, false);
            } else {
                throw new LedpException(LedpCode.LEDP_40011, new String[] { aiModel.getId() });
            }
        } else {
            txn = new Bucket.Transaction(productIds, TimeFilter.ever(), spentFilter, unitsFilter, true);
        }
        Bucket txnBucket = Bucket.txnBkt(txn);
        productTxnRestriction = new BucketRestriction(attrLookup, txnBucket);
    }
}
