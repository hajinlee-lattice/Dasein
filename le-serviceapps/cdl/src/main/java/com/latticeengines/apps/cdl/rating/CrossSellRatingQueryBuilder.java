package com.latticeengines.apps.cdl.rating;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.graph.traversal.impl.DepthFirstSearch;
import com.latticeengines.domain.exposed.cdl.ModelingQueryType;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.CrossSellModelingConfig;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.RestrictionBuilder;
import com.latticeengines.domain.exposed.query.TransactionRestriction;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;

public abstract class CrossSellRatingQueryBuilder implements RatingQueryBuilder {

    private int targetPeriodId;
    protected AIModel aiModel;
    protected MetadataSegment baseSegment;
    protected String productIds;
    protected Restriction productTxnRestriction;
    protected EventFrontEndQuery ratingFrontEndQuery;
    protected int queryEvaluationId;
    protected String periodTypeName = PeriodStrategy.Template.Month.name();

    protected abstract void handleCustomSegment();

    protected abstract void handleProxyProducts();

    protected abstract void buildProductTransactionRestrictions();

    protected abstract void handleCustomTrainingPeriod();

    protected abstract void setQueryEvaluationId();

    public final EventFrontEndQuery build() {
        handleCustomSegment();
        handleProxyProducts();
        replaceTransactionRestrictionsWithBucketRestrictions();
        removeTimeWindowRestrictions();
        buildProductTransactionRestrictions();
        setQueryEvaluationId();
        buildRatingFrontEndQuery();
        handleCustomTrainingPeriod();
        return ratingFrontEndQuery;
    }

    protected void removeTimeWindowRestrictions() {
        DepthFirstSearch dfs = new DepthFirstSearch();
        if (baseSegment.getAccountRestriction() != null) {
            Restriction accRestriction = baseSegment.getAccountRestriction();
            dfs.run(accRestriction, (object, ctx) -> {
                GraphNode node = (GraphNode) object;
                if (node instanceof BucketRestriction && ((BucketRestriction) node).getBkt().getTransaction() != null) {
                    Bucket.Transaction transaction = ((BucketRestriction) node).getBkt().getTransaction();
                    if (transaction.getTimeFilter().getRelation() == ComparisonType.EVER && !transaction.getNegate()) {
                        ((BucketRestriction) node).setIgnored(false);
                    } else {
                        ((BucketRestriction) node).setIgnored(true);
                    }
                }
            });
            baseSegment.setAccountRestriction(accRestriction);
        }
    }

    private void replaceTransactionRestrictionsWithBucketRestrictions() {
        DepthFirstSearch dfs = new DepthFirstSearch();
        if (baseSegment.getAccountRestriction() != null) {
            Restriction accRestriction = baseSegment.getAccountRestriction();
            dfs.run(accRestriction, (object, ctx) -> {
                if (object instanceof LogicalRestriction
                        && CollectionUtils.isNotEmpty(((LogicalRestriction) object).getChildren()) //
                        && ((LogicalRestriction) object).getChildren().stream()
                                .anyMatch(c -> c instanceof TransactionRestriction)) {
                    LogicalRestriction node = (LogicalRestriction) object;
                    List<Restriction> newList = new ArrayList<>();
                    boolean anyChildUpdated = false;
                    for (GraphNode child : node.getChildren()) {
                        if (child instanceof TransactionRestriction) {
                            TransactionRestriction trxChild = (TransactionRestriction) child;
                            Bucket bkt = Bucket
                                    .txnBkt(new Bucket.Transaction(trxChild.getProductId(), trxChild.getTimeFilter(),
                                            trxChild.getSpentFilter(), trxChild.getUnitFilter(), trxChild.isNegate()));
                            BucketRestriction bktRestriction = new BucketRestriction(
                                    new AttributeLookup(BusinessEntity.Transaction, trxChild.getProductId()), bkt);
                            bktRestriction.setIgnored(true);
                            newList.add(bktRestriction);
                            anyChildUpdated = true;
                        } else {
                            newList.add((Restriction) child);
                        }
                    }
                    if (anyChildUpdated) {
                        node.setRestrictions(newList);
                    }
                }
            });
            baseSegment.setAccountRestriction(accRestriction);
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
        ratingFrontEndQuery.setPeriodName(periodTypeName);
        ratingFrontEndQuery.setTargetProductIds(getProductsAsList());
        ratingFrontEndQuery.setEvaluationPeriodId(queryEvaluationId);
    }

    private List<String> getProductsAsList() {
        List<String> productList = new ArrayList<>();
        if (StringUtils.isNotBlank(productIds)) {
            productList = Arrays.asList(productIds.split("\\s*,\\s*"));
        }
        return productList;
    }

    protected CrossSellRatingQueryBuilder(RatingEngine ratingEngine, AIModel aiModel, int targetPeriodId) {
        this.baseSegment = (MetadataSegment) ratingEngine.getSegment().clone();
        this.aiModel = aiModel;
        CrossSellModelingConfig config = CrossSellModelingConfig.getAdvancedModelingConfig(aiModel);
        this.productIds = String.join(",", config.getTargetProducts());
        this.targetPeriodId = targetPeriodId;
    }

    protected int getTargetPeriodId() {
        return targetPeriodId;
    }

    public static RatingQueryBuilder getCrossSellRatingQueryBuilder(RatingEngine ratingEngine, AIModel aiModel,
            ModelingQueryType modelingQueryType, int targetPeriodId) {
        switch (modelingQueryType) {
        case TARGET:
            return new CrossSellRatingTargetQueryBuilder(ratingEngine, aiModel, targetPeriodId);
        case TRAINING:
            return new CrossSellRatingTrainingQueryBuilder(ratingEngine, aiModel, targetPeriodId);
        case EVENT:
            return new CrossSellRatingEventQueryBuilder(ratingEngine, aiModel, targetPeriodId);
        default:
            throw new LedpException(LedpCode.LEDP_40010, new String[] { modelingQueryType.getModelingQueryTypeName() });
        }
    }

    protected CrossSellModelingConfig getAdvancedConfig() {
        return (CrossSellModelingConfig) aiModel.getAdvancedModelingConfig();
    }
}
