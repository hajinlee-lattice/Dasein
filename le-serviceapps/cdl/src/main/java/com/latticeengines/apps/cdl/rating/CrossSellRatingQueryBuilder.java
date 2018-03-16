package com.latticeengines.apps.cdl.rating;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.graph.traversal.impl.DepthFirstSearch;
import com.latticeengines.domain.exposed.cdl.ModelingQueryType;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
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
import com.latticeengines.proxy.exposed.cdl.PeriodProxy;

public abstract class CrossSellRatingQueryBuilder implements RatingQueryBuilder {

    protected AIModel aiModel;
    protected MetadataSegment baseSegment;
    protected String productIds;
    protected Restriction productTxnRestriction;
    protected EventFrontEndQuery ratingFrontEndQuery;
    private int evaluationPeriod;

    @Inject
    protected PeriodProxy periodProxy;

    protected abstract void handleCustomSegment();

    protected abstract void handleProxyProducts();

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

    protected void removeTimeWindowRestrictions() {
        DepthFirstSearch dfs = new DepthFirstSearch();
        if (baseSegment.getAccountRestriction() != null) {
            dfs.run(baseSegment.getAccountRestriction(), (object, ctx) -> {
                GraphNode node = (GraphNode) object;
                if (node instanceof BucketRestriction && ((BucketRestriction) node).getBkt().getTransaction() != null) {
                    Bucket.Transaction transaction = ((BucketRestriction) node).getBkt().getTransaction();
                    if (transaction.getNegate() && transaction.getTimeFilter().getRelation() != ComparisonType.EVER) {
                        ((BucketRestriction) node).setIgnored(true);
                    }
                }
            });
        }
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
        ratingFrontEndQuery.setEvaluationPeriodId(evaluationPeriod);
    }

    private List<String> getProductsAsList() {
        List<String> productList = new ArrayList<>();
        if (StringUtils.isNotBlank(productIds)) {
            productList = Arrays.asList(productIds.split("\\s*,\\s*"));
        }
        return productList;
    }

    protected CrossSellRatingQueryBuilder(RatingEngine ratingEngine, AIModel aiModel, int evaluationPeriod) {
        this.baseSegment = (MetadataSegment) ratingEngine.getSegment().clone();
        this.aiModel = aiModel;
        this.productIds = String.join(",", aiModel.getTargetProducts());
        this.evaluationPeriod = evaluationPeriod;
    }

    public static RatingQueryBuilder getCrossSellRatingQueryBuilder(RatingEngine ratingEngine, AIModel aiModel,
            ModelingQueryType modelingQueryType, int evaluationPeriod) {
        switch (modelingQueryType) {
        case TARGET:
            return new CrossSellRatingTargetQueryBuilder(ratingEngine, aiModel, evaluationPeriod);
        case TRAINING:
            return new CrossSellRatingTrainingQueryBuilder(ratingEngine, aiModel, evaluationPeriod);
        case EVENT:
            return new CrossSellRatingEventQueryBuilder(ratingEngine, aiModel, evaluationPeriod);
        default:
            throw new LedpException(LedpCode.LEDP_40010, new String[] { modelingQueryType.getModelingQueryTypeName() });
        }
    }
}
