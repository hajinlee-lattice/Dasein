package com.latticeengines.apps.cdl.rating;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.CrossSellModelingConfigKeys;
import com.latticeengines.domain.exposed.pls.ModelingConfigFilter;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.CrossSellModelingConfig;
import com.latticeengines.domain.exposed.query.AggregationFilter;
import com.latticeengines.domain.exposed.query.AggregationSelector;
import com.latticeengines.domain.exposed.query.AggregationType;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.query.TransactionRestriction;
import org.apache.commons.collections4.CollectionUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class CrossSellRatingEventQueryBuilder extends CrossSellRatingQueryBuilder {

    protected CrossSellRatingEventQueryBuilder(RatingEngine ratingEngine, AIModel aiModel, String periodTypeName,
            int evaluationPeriod, Set<String> attributeMetadata) {
        super(ratingEngine, aiModel, periodTypeName, evaluationPeriod, attributeMetadata);
    }

    @Override
    protected void handleCustomSegment() {
        if (aiModel.getTrainingSegment() != null) {
            baseSegment = (MetadataSegment) aiModel.getTrainingSegment().clone();
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
        TransactionRestriction unitsFilterRestriction = null;
        TransactionRestriction spentFilterRestriction = null;
        TransactionRestriction crossSellRestriction = null;

        TimeFilter nextPeriodTimeFilter = new TimeFilter(ComparisonType.FOLLOWING, periodTypeName, Arrays.asList(1, 1));
        TransactionRestriction purchasedInNextPeriod = new TransactionRestriction(productIds, nextPeriodTimeFilter,
                false, null, null);

        CrossSellModelingConfig advancedConf = (CrossSellModelingConfig) aiModel.getAdvancedModelingConfig();
        Map<CrossSellModelingConfigKeys, ModelingConfigFilter> filters = advancedConf.getFilters();

        if (filters.containsKey(CrossSellModelingConfigKeys.SPEND_IN_PERIOD)) {
            ModelingConfigFilter configFilter = filters.get(CrossSellModelingConfigKeys.SPEND_IN_PERIOD);
            AggregationFilter spentFilter = new AggregationFilter(AggregationSelector.SPENT,
                    AggregationType.AT_LEAST_ONCE, configFilter.getCriteria(),
                    Collections.singletonList(configFilter.getValue()));
            spentFilterRestriction = new TransactionRestriction(productIds, nextPeriodTimeFilter, false, spentFilter,
                    null);
        }

        if (filters.containsKey(CrossSellModelingConfigKeys.QUANTITY_IN_PERIOD)) {
            ModelingConfigFilter configFilter = filters.get(CrossSellModelingConfigKeys.QUANTITY_IN_PERIOD);
            AggregationFilter unitsFilter = new AggregationFilter(AggregationSelector.UNIT,
                    AggregationType.AT_LEAST_ONCE, configFilter.getCriteria(),
                    Collections.singletonList(configFilter.getValue()));
            unitsFilterRestriction = new TransactionRestriction(productIds, nextPeriodTimeFilter, false, null,
                    unitsFilter);
        }

        switch (advancedConf.getModelingStrategy()) {
            case CROSS_SELL_REPEAT_PURCHASE:
                ModelingConfigFilter configFilter = filters.get(CrossSellModelingConfigKeys.PURCHASED_BEFORE_PERIOD);
                if (configFilter == null) {
                    throw new LedpException(LedpCode.LEDP_40011, new String[] { aiModel.getId() });
                }

                // Not Purchased In Past X Periods
                crossSellRestriction = (configFilter.getValue() == null || configFilter.getValue() < 1) ? null
                        : new TransactionRestriction(productIds,
                                TimeFilter.priorOnly(configFilter.getValue() - 1, periodTypeName), false, null, null);
                break;
            case CROSS_SELL_FIRST_PURCHASE:
                // Never Purchased Including Current Period
                crossSellRestriction = new TransactionRestriction(productIds, TimeFilter.ever(periodTypeName), true,
                        null, null);
                break;
            default:
                throw new LedpException(LedpCode.LEDP_40017);
        }

        productTxnRestriction = Restriction.builder() //
                .and(crossSellRestriction, purchasedInNextPeriod, spentFilterRestriction, unitsFilterRestriction) //
                .build();
    }

    @Override
    protected void setQueryEvaluationId() {
        evaluationPeriodId = getTargetPeriodId() - 1;
    }
}
