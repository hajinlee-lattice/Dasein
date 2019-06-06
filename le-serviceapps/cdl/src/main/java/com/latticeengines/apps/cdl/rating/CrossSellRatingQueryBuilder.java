package com.latticeengines.apps.cdl.rating;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
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
import com.latticeengines.domain.exposed.pls.cdl.rating.model.CrossSellModelingConfig;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.TransactionRestriction;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;

public abstract class CrossSellRatingQueryBuilder implements RatingQueryBuilder {

    final String periodTypeName;
    private final int targetPeriodId;
    protected AIModel aiModel;
    MetadataSegment baseSegment;
    String productIds;
    Restriction productTxnRestriction;
    EventFrontEndQuery ratingFrontEndQuery;
    int evaluationPeriodId;
    private Set<String> dateAttributes;

    private List<ComparisonType> comparisonTypesToBeIgnored = Arrays.asList(ComparisonType.BEFORE, ComparisonType.AFTER,
            ComparisonType.BETWEEN_DATE);

    CrossSellRatingQueryBuilder(RatingEngine ratingEngine, AIModel aiModel, String periodTypeName, int targetPeriodId,
            Set<String> attributeMetadata) {
        this.baseSegment = (MetadataSegment) ratingEngine.getSegment().clone();
        this.aiModel = aiModel;
        CrossSellModelingConfig config = CrossSellModelingConfig.getAdvancedModelingConfig(aiModel);
        if (CollectionUtils.isEmpty(config.getTargetProducts())) {
            throw new IllegalArgumentException("AI Model " + aiModel.getId() //
                    + " has empty target products list, which is not allowed.");
        }
        this.productIds = String.join(",", config.getTargetProducts());
        this.targetPeriodId = targetPeriodId;
        this.periodTypeName = periodTypeName;
        this.dateAttributes = attributeMetadata;
    }

    protected abstract void handleCustomSegment();

    protected abstract void handleProxyProducts();

    protected abstract void buildProductTransactionRestrictions();

    protected abstract void handleCustomTrainingPeriod();

    protected abstract void setQueryEvaluationId();

    protected FrontEndQuery getAccountFiltererSegmentQuery() {
        return null;
    }

    protected List<ComparisonType> getComparisonTypesToBeIgnored() {
        return comparisonTypesToBeIgnored;
    }

    public final EventFrontEndQuery build() {
        handleCustomSegment();
        handleProxyProducts();
        replaceTransactionRestrictionsWithBucketRestrictions();
        deactivateTimeWindowRestrictions();
        deactivateDateTimeRestrictions();
        buildProductTransactionRestrictions();
        setQueryEvaluationId();
        buildRatingFrontEndQuery();
        handleCustomTrainingPeriod();
        return ratingFrontEndQuery;
    }

    protected void deactivateTimeWindowRestrictions() {
        DepthFirstSearch dfs = new DepthFirstSearch();
        if (baseSegment.getAccountRestriction() != null) {
            Restriction accRestriction = baseSegment.getAccountRestriction();
            dfs.run(accRestriction, (object, ctx) -> {
                GraphNode node = (GraphNode) object;
                if (node instanceof BucketRestriction && ((BucketRestriction) node).getBkt().getTransaction() != null) {
                    Bucket.Transaction transaction = ((BucketRestriction) node).getBkt().getTransaction();
                    if (getComparisonTypesToBeIgnored().contains(transaction.getTimeFilter().getRelation())) {
                        ((BucketRestriction) node).setIgnored(true);
                    }
                }
            });
            baseSegment.setAccountRestriction(accRestriction);
        }
    }

    protected void deactivateDateTimeRestrictions() {
        DepthFirstSearch dfs = new DepthFirstSearch();
        if (baseSegment.getAccountRestriction() != null) {
            Restriction accRestriction = baseSegment.getAccountRestriction();
            dfs.run(accRestriction, (object, ctx) -> {
                GraphNode node = (GraphNode) object;
                if (node instanceof BucketRestriction) {
                    String attrName = ((BucketRestriction) node).getAttr().getAttribute();
                    if (dateAttributes.contains(attrName)) {
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
        Restriction finalQueryRestriction = buildFinalQueryRestriction();

        MetadataSegment querySegment = baseSegment;
        querySegment.setAccountFrontEndRestriction(new FrontEndRestriction(finalQueryRestriction));
        querySegment.setAccountRestriction(finalQueryRestriction);

        ratingFrontEndQuery = EventFrontEndQuery.fromSegment(querySegment);
        ratingFrontEndQuery.setPeriodName(periodTypeName);
        ratingFrontEndQuery.setTargetProductIds(getProductsAsList());
        ratingFrontEndQuery.setEvaluationPeriodId(evaluationPeriodId);
        ratingFrontEndQuery.setSegmentQuery(getAccountFiltererSegmentQuery());
        ratingFrontEndQuery.setContactRestriction(null);
    }

    protected Restriction buildFinalQueryRestriction() {
        return Restriction.builder().and(baseSegment.getAccountRestriction(), productTxnRestriction).build();
    }

    private List<String> getProductsAsList() {
        List<String> productList = new ArrayList<>();
        if (StringUtils.isNotBlank(productIds)) {
            productList = Arrays.asList(productIds.split("\\s*,\\s*"));
        }
        return productList;
    }

    protected int getTargetPeriodId() {
        return targetPeriodId;
    }

    public static RatingQueryBuilder getCrossSellRatingQueryBuilder(RatingEngine ratingEngine, AIModel aiModel,
            ModelingQueryType modelingQueryType, String periodTypeName, int targetPeriodId,
            Set<String> attributeMetadata) {
        switch (modelingQueryType) {
            case TARGET:
                return new CrossSellRatingTargetQueryBuilder(ratingEngine, aiModel, periodTypeName, targetPeriodId,
                        attributeMetadata);
            case TRAINING:
                return new CrossSellRatingTrainingQueryBuilder(ratingEngine, aiModel, periodTypeName, targetPeriodId,
                        attributeMetadata);
            case EVENT:
                return new CrossSellRatingEventQueryBuilder(ratingEngine, aiModel, periodTypeName, targetPeriodId,
                        attributeMetadata);
            default:
                throw new LedpException(LedpCode.LEDP_40010,
                        new String[] { modelingQueryType.getModelingQueryTypeName() });
        }
    }

    protected CrossSellModelingConfig getAdvancedConfig() {
        return (CrossSellModelingConfig) aiModel.getAdvancedModelingConfig();
    }
}
