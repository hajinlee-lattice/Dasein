package com.latticeengines.apps.cdl.rating;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.ModelingQueryType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.RestrictionBuilder;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;

public abstract class CrossSellRatingQueryBuilder implements RatingQueryBuilder {

    protected AIModel aiModel;
    protected MetadataSegment baseSegment;
    protected String productIds;
    protected BucketRestriction productTxnRestriction;
    protected EventFrontEndQuery ratingFrontEndQuery;

    protected abstract void handleCustomSegment();

    protected abstract void handleProxyProducts();

    protected abstract void removeTimeWindowRestrictions();

    protected abstract void buildProductTransactionRestrictions();

    protected abstract void handleCustomTrainingPeriod();

    public final EventFrontEndQuery build() {
        handleCustomSegment();
        handleProxyProducts();
        removeTimeWindowRestrictions();
        buildProductTransactionRestrictions();
        buildRatingFrontEndQuery();
        handleCustomTrainingPeriod();
        return ratingFrontEndQuery;
    }

    private void buildRatingFrontEndQuery() {
        RestrictionBuilder restrictionBuilder = Restriction.builder();

        Restriction finalQueryRestriction = restrictionBuilder
                .and(baseSegment.getAccountRestriction(), productTxnRestriction).build();

        MetadataSegment querySegment = baseSegment;
        querySegment.setAccountFrontEndRestriction(new FrontEndRestriction(finalQueryRestriction));
        querySegment.setAccountRestriction(finalQueryRestriction);

        ratingFrontEndQuery = EventFrontEndQuery.fromSegment(querySegment);
        ratingFrontEndQuery.setPeriodName(TimeFilter.Period.Month.name());
        ratingFrontEndQuery.setTargetProductIds(getProductsAsList());
    }

    private List<String> getProductsAsList() {
        List<String> productList = new ArrayList<>();
        if (StringUtils.isNotBlank(productIds)) {
            List<?> attrListIntermediate = JsonUtils.deserialize(productIds, List.class);
            productList = JsonUtils.convertList(attrListIntermediate, String.class);
        }
        return productList;
    }

    protected CrossSellRatingQueryBuilder(RatingEngine ratingEngine, AIModel aiModel) {
        this.baseSegment = (MetadataSegment) ratingEngine.getSegment().clone();
        this.aiModel = aiModel;
        this.productIds = String.join(",", aiModel.getTargetProducts());
    }

    public static RatingQueryBuilder getCrossSellRatingQueryBuilder(RatingEngine ratingEngine, AIModel aiModel,
            ModelingQueryType modelingQueryType) {
        switch (modelingQueryType) {
        case TARGET:
            return new CrossSellRatingTargetQueryBuilder(ratingEngine, aiModel);
        case TRAINING:
            return new CrossSellRatingTrainingQueryBuilder(ratingEngine, aiModel);
        case EVENT:
            return new CrossSellRatingEventQueryBuilder(ratingEngine, aiModel);
        default:
            throw new LedpException(LedpCode.LEDP_40010, new String[] { modelingQueryType.getModelingQueryTypeName() });
        }
    }

    protected TimeFilter priorOnly(int value) {
        TimeFilter filter = new TimeFilter(ComparisonType.PRIOR_ONLY, Collections.singletonList(value));
        filter.setPeriod(TimeFilter.Period.Month.name());
        return filter;
    }
}
