package com.latticeengines.apps.cdl.rating;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.CrossSellModelingConfigKeys;
import com.latticeengines.domain.exposed.pls.ModelingConfigFilter;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.CrossSellModelingConfig;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.query.TransactionRestriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;

import java.util.Map;
import java.util.Set;

public class CrossSellRatingTargetQueryBuilder extends CrossSellRatingQueryBuilder {

    protected MetadataSegment accountFiltererSegment;

    protected CrossSellRatingTargetQueryBuilder(RatingEngine ratingEngine, AIModel aiModel, String periodTypeName,
            int evaluationPeriod, Set<String> attributeMetadata) {
        super(ratingEngine, aiModel, periodTypeName, evaluationPeriod, attributeMetadata);
        accountFiltererSegment = (MetadataSegment) ratingEngine.getSegment().clone();
    }

    @Override
    protected void handleCustomSegment() {
    }

    @Override
    protected void handleProxyProducts() {
    }

    @Override
    protected void buildProductTransactionRestrictions() {
        CrossSellModelingConfig advancedConf = (CrossSellModelingConfig) aiModel.getAdvancedModelingConfig();

        Map<CrossSellModelingConfigKeys, ModelingConfigFilter> filters = //
                advancedConf.getFilters();

        switch (advancedConf.getModelingStrategy()) {

            case CROSS_SELL_REPEAT_PURCHASE:
                ModelingConfigFilter config = filters.get(CrossSellModelingConfigKeys.PURCHASED_BEFORE_PERIOD);
                if (config == null) {
                    throw new LedpException(LedpCode.LEDP_40011, new String[] { aiModel.getId() });
                }
                productTxnRestriction = (config.getValue() == null || config.getValue() < 1) ? null
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
    protected void handleCustomTrainingPeriod() {
        // Do nothing for target query
    }

    @Override
    protected FrontEndQuery getAccountFiltererSegmentQuery() {
        return FrontEndQuery.fromSegment(accountFiltererSegment);
    }

    @Override
    protected void setQueryEvaluationId() {
        evaluationPeriodId = getTargetPeriodId();
    }

    @Override
    protected Restriction buildFinalQueryRestriction() {
        return productTxnRestriction;
    }
}
