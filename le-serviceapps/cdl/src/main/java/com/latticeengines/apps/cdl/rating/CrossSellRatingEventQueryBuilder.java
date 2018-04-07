package com.latticeengines.apps.cdl.rating;

import java.util.Collections;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;

import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.CrossSellModelingConfigKeys;
import com.latticeengines.domain.exposed.pls.ModelingConfigFilter;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.CrossSellModelingConfig;
import com.latticeengines.domain.exposed.query.AggregationFilter;
import com.latticeengines.domain.exposed.query.AggregationSelector;
import com.latticeengines.domain.exposed.query.AggregationType;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.TimeFilter;

public class CrossSellRatingEventQueryBuilder extends CrossSellRatingQueryBuilder {

    protected CrossSellRatingEventQueryBuilder(RatingEngine ratingEngine, AIModel aiModel, int evaluationPeriod) {
        super(ratingEngine, aiModel, evaluationPeriod);
    }

    @Override
    protected void handleCustomSegment() {
        if (aiModel.getTrainingSegment() != null) {
            baseSegment = aiModel.getTrainingSegment();
        }
    }

    @Override
    protected void handleProxyProducts() {
        CrossSellModelingConfig config = CrossSellModelingConfig.getAdvancedModelingConfig(aiModel);
        if (!CollectionUtils.isEmpty(config.getTrainingProducts())) {
            productIds = String.join(",", config.getTrainingProducts());
        }
    }

    @Override
    protected void handleCustomTrainingPeriod() {
        ModelingConfigFilter filter = CrossSellModelingConfig.getAdvancedModelingConfig(aiModel).getFilters()
                .get(CrossSellModelingConfigKeys.TRAINING_SET_PERIOD);
        if (filter != null) {
            ratingFrontEndQuery.setPeriodCount(filter.getValue());
        }
    }

    @Override
    protected void buildProductTransactionRestrictions() {
        AttributeLookup attrLookup = new AttributeLookup(BusinessEntity.Transaction, productIds);
        AggregationFilter unitsFilter = null;
        AggregationFilter spentFilter = null;

        CrossSellModelingConfig advancedConf = (CrossSellModelingConfig) aiModel.getAdvancedModelingConfig();

        Map<CrossSellModelingConfigKeys, ModelingConfigFilter> filters = //
                advancedConf.getFilters();

        if (filters.containsKey(CrossSellModelingConfigKeys.QUANTITY_IN_PERIOD)) {
            ModelingConfigFilter configFilter = filters.get(CrossSellModelingConfigKeys.QUANTITY_IN_PERIOD);
            unitsFilter = new AggregationFilter(AggregationSelector.UNIT, AggregationType.AT_LEAST_ONCE,
                    configFilter.getCriteria(), Collections.singletonList(configFilter.getValue()));
        }

        if (filters.containsKey(CrossSellModelingConfigKeys.SPEND_IN_PERIOD)) {
            ModelingConfigFilter configFilter = filters.get(CrossSellModelingConfigKeys.SPEND_IN_PERIOD);
            spentFilter = new AggregationFilter(AggregationSelector.SPENT, AggregationType.AT_LEAST_ONCE,
                    configFilter.getCriteria(), Collections.singletonList(configFilter.getValue()));
        }

        Bucket.Transaction txn;
        switch (advancedConf.getModelingStrategy()) {
        case CROSS_SELL_REPEAT_PURCHASE:
            ModelingConfigFilter configFilter = filters.get(CrossSellModelingConfigKeys.PURCHASED_BEFORE_PERIOD);
            if (configFilter == null) {
                throw new LedpException(LedpCode.LEDP_40011, new String[] { aiModel.getId() });
            }

            productTxnRestriction = new BucketRestriction(attrLookup,
                    Bucket.txnBkt(new Bucket.Transaction(productIds,
                            TimeFilter.priorOnly(configFilter.getValue() - 1, PeriodStrategy.Template.Month.name()),
                            null, null, false)));
            break;
        case CROSS_SELL_FIRST_PURCHASE:
            if (unitsFilter == null && spentFilter == null) {
                txn = new Bucket.Transaction(productIds, TimeFilter.ever(), null, null, true);
                productTxnRestriction = new BucketRestriction(attrLookup, Bucket.txnBkt(txn));
            } else {
                Bucket txnBkt1 = Bucket.txnBkt(new Bucket.Transaction(productIds,
                        TimeFilter.priorOnly(1, PeriodStrategy.Template.Month.name()), null, null, true));
                Bucket txnBkt2 = Bucket.txnBkt(new Bucket.Transaction(productIds,
                        new TimeFilter(ComparisonType.IN_CURRENT_PERIOD, PeriodStrategy.Template.Month.name(), null),
                        spentFilter, unitsFilter, false));
                productTxnRestriction = Restriction.builder()
                        .and(new BucketRestriction(attrLookup, txnBkt1), new BucketRestriction(attrLookup, txnBkt2))
                        .build();
            }
            break;
        default:
            throw new LedpException(LedpCode.LEDP_40017);
        }

    }

    @Override
    protected void setQueryEvaluationId() {
        queryEvaluationId = getTargetPeriodId() - 1;
    }
}
