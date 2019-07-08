package com.latticeengines.apps.cdl.rating;

import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.CrossSellModelingConfigKeys;
import com.latticeengines.domain.exposed.pls.ModelingConfigFilter;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.CrossSellModelingConfig;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.query.TransactionRestriction;

public class CrossSellRatingTrainingQueryBuilder extends CrossSellRatingQueryBuilder {

    protected CrossSellRatingTrainingQueryBuilder(RatingEngine ratingEngine, AIModel aiModel, String periodTypeName,
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
        if (CollectionUtils.isEmpty(getAdvancedConfig().getTrainingProducts()))
            return;
        productIds = String.join(",", getAdvancedConfig().getTrainingProducts());
    }

    @Override
    protected void handleCustomTrainingPeriod() {
        ModelingConfigFilter filter = getAdvancedConfig().getFilters()
                .get(CrossSellModelingConfigKeys.TRAINING_SET_PERIOD);
        if (filter != null) {
            ratingFrontEndQuery.setPeriodCount(filter.getValue());
        }
    }

    @Override
    protected void buildProductTransactionRestrictions() {
        CrossSellModelingConfig advancedConf = getAdvancedConfig();

        Map<CrossSellModelingConfigKeys, ModelingConfigFilter> filters = //
                advancedConf.getFilters();

        switch (advancedConf.getModelingStrategy()) {
            case CROSS_SELL_REPEAT_PURCHASE:
                ModelingConfigFilter config = filters.get(CrossSellModelingConfigKeys.PURCHASED_BEFORE_PERIOD);
                if (config == null) {
                    throw new LedpException(LedpCode.LEDP_40011, new String[] { aiModel.getId() });
                }
                productTxnRestriction = (config.getValue() == null || config.getValue() < 1)
                        ? new TransactionRestriction(productIds, TimeFilter.ever(periodTypeName), false, null, null)
                        : new TransactionRestriction(productIds,
                                TimeFilter.priorOnly(config.getValue() - 1, periodTypeName), false, null, null);
                break;
            case CROSS_SELL_FIRST_PURCHASE:
                productTxnRestriction = new TransactionRestriction(productIds, TimeFilter.ever(periodTypeName), true,
                        null, null);
                break;
            default:
                throw new LedpException(LedpCode.LEDP_40017);
        }
    }

    @Override
    protected void setQueryEvaluationId() {
        evaluationPeriodId = getTargetPeriodId() - 1;
    }
}
