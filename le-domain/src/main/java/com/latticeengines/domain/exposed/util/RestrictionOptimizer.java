package com.latticeengines.domain.exposed.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;

import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.domain.exposed.query.DateRestriction;
import com.latticeengines.domain.exposed.query.ExistsRestriction;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.MetricRestriction;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.TransactionRestriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;

public final class RestrictionOptimizer {

    protected RestrictionOptimizer() {
        throw new UnsupportedOperationException();
    }

    public static void optimize(FrontEndQuery frontEndQuery) {
        if (frontEndQuery.getAccountRestriction() != null) {
            Restriction restriction = frontEndQuery.getAccountRestriction().getRestriction();
            if (restriction != null) {
                frontEndQuery.getAccountRestriction().setRestriction(RestrictionOptimizer.optimize(restriction));
            }
        }
        if (frontEndQuery.getContactRestriction() != null) {
            Restriction restriction = frontEndQuery.getContactRestriction().getRestriction();
            if (restriction != null) {
                frontEndQuery.getContactRestriction().setRestriction(RestrictionOptimizer.optimize(restriction));
            }
        }
    }

    public static Restriction optimize(Restriction restriction) {
        if (restriction == null) {
            return null;
        } else if (restriction instanceof ConcreteRestriction //
                || restriction instanceof TransactionRestriction //
                || restriction instanceof DateRestriction //
                || restriction instanceof MetricRestriction) {
            return restriction;
        } else if (restriction instanceof ExistsRestriction) {
            ExistsRestriction eR = (ExistsRestriction) restriction;
            if (eR.getRestriction() != null) {
                Restriction innerRes = eR.getRestriction();
                eR.setRestriction(optimize(innerRes));
            }
            return eR;
        } else if (restriction instanceof BucketRestriction) {
            return optimizeBucketRestriction((BucketRestriction) restriction);
        } else if (restriction instanceof LogicalRestriction) {
            return optimizeLogicalRestriction((LogicalRestriction) restriction);
        } else {
            throw new RuntimeException("Cannot optimize restriction of type " + restriction.getClass());
        }
    }

    private static BucketRestriction optimizeBucketRestriction(BucketRestriction bucket) {
        bucket.setSelected(null);
        if (Boolean.TRUE.equals(bucket.getIgnored())) {
            return null;
        } else {
            bucket.setIgnored(null);
            return bucket;
        }
    }

    private static Restriction optimizeLogicalRestriction(LogicalRestriction logicalRestriction) {
        if (CollectionUtils.isEmpty(logicalRestriction.getRestrictions())) {
            return null;
        }

        List<Restriction> children = new ArrayList<>();
        logicalRestriction.getRestrictions().forEach(restriction -> {
            if (restriction != null) {
                Restriction flatRestriction;
                if (restriction instanceof LogicalRestriction || restriction instanceof BucketRestriction) {
                    flatRestriction = optimize(restriction);
                } else {
                    flatRestriction = restriction;
                }
                if (flatRestriction != null) {
                    if (flatRestriction instanceof LogicalRestriction) {
                        LogicalRestriction flattenLogic = (LogicalRestriction) flatRestriction;
                        if (flattenLogic.getOperator().equals(logicalRestriction.getOperator())) {
                            children.addAll(((LogicalRestriction) flatRestriction).getRestrictions());
                        } else {
                            children.add(flattenLogic);
                        }
                    } else {
                        children.add(flatRestriction);
                    }
                }
            }
        });

        logicalRestriction.setRestrictions(mergeListOperations(children));

        if (children.isEmpty()) {
            return null;
        } else if (logicalRestriction.getRestrictions().size() == 1) {
            return logicalRestriction.getRestrictions().get(0);
        } else {
            return logicalRestriction;
        }
    }

    public static Restriction groupMetrics(Restriction restriction) {
        Restriction optimized = restriction;
        if (restriction instanceof ConcreteRestriction || restriction instanceof BucketRestriction) {
            BusinessEntity metricEntity = getMetricEntity(restriction);
            if (metricEntity != null) {
                MetricRestriction metricRestriction = new MetricRestriction();
                metricRestriction.setMetricEntity(metricEntity);
                metricRestriction.setRestriction(restriction);
                optimized = metricRestriction;
            }
        } else if (restriction instanceof LogicalRestriction) {
            List<Restriction> children = ((LogicalRestriction) restriction).getRestrictions();
            if (CollectionUtils.isNotEmpty(children)) {
                Map<BusinessEntity, List<Restriction>> groupByMetricEntity = new HashMap<>();
                List<Restriction> newChildren = new ArrayList<>();
                for (Restriction child : children) {
                    Restriction modifiedChild = child;
                    if (child instanceof LogicalRestriction) {
                        modifiedChild = groupMetrics(child);
                    }
                    BusinessEntity metricEntity = getMetricEntity(modifiedChild);
                    if (metricEntity != null) {
                        if (!groupByMetricEntity.containsKey(metricEntity)) {
                            groupByMetricEntity.put(metricEntity, new ArrayList<>());
                        }
                        groupByMetricEntity.get(metricEntity).add(modifiedChild);
                    } else {
                        newChildren.add(child);
                    }
                }
                if (MapUtils.isNotEmpty(groupByMetricEntity)) {
                    groupByMetricEntity.forEach((entity, restrictions) -> {
                        LogicalRestriction logicalRestriction = new LogicalRestriction();
                        logicalRestriction.setOperator(((LogicalRestriction) restriction).getOperator());
                        List<Restriction> innerRestrictions = new ArrayList<>();
                        restrictions.forEach(r -> {
                            if (r instanceof MetricRestriction) {
                                innerRestrictions.add(((MetricRestriction) r).getRestriction());
                            } else {
                                innerRestrictions.add(r);
                            }
                        });
                        logicalRestriction.setRestrictions(innerRestrictions);

                        MetricRestriction metricRestriction = new MetricRestriction();
                        metricRestriction.setMetricEntity(entity);
                        metricRestriction.setRestriction(optimize(logicalRestriction));
                        newChildren.add(metricRestriction);
                    });
                }
                ((LogicalRestriction) restriction).setRestrictions(newChildren);
                if (((LogicalRestriction) restriction).getRestrictions().size() > 1) {
                    optimized = restriction;
                } else {
                    optimized = ((LogicalRestriction) restriction).getRestrictions().get(0);
                }
            } else {
                optimized = null;
            }
        }
        return optimized;
    }

    private static BusinessEntity getMetricEntity(Restriction restriction) {
        if (restriction instanceof MetricRestriction) {
            MetricRestriction metricRestriction = (MetricRestriction) restriction;
            return metricRestriction.getMetricEntity();
        } else if (restriction instanceof ConcreteRestriction) {
            ConcreteRestriction concreteRestriction = (ConcreteRestriction) restriction;
            Lookup lookup = concreteRestriction.getLhs();
            if (lookup instanceof AttributeLookup) {
                AttributeLookup attributeLookup = (AttributeLookup) lookup;
                BusinessEntity entity = attributeLookup.getEntity();
                if (BusinessEntity.DepivotedPurchaseHistory.equals(entity)) {
                    return entity;
                }
            }
        }
        return null;
    }

    private static List<Restriction> mergeListOperations(List<Restriction> clauses) {
        Map<ImmutablePair<AttributeLookup, ComparisonType>, List<Object>> listVals = new HashMap<>();
        List<Restriction> merged = new ArrayList<>();
        for (Restriction clause: clauses) {
            boolean shouldMerge = false;
            if (clause instanceof BucketRestriction) {
                BucketRestriction bucketRestriction = (BucketRestriction) clause;
                ComparisonType operator = bucketRestriction.getBkt().getComparisonType();
                if (RestrictionUtils.isMultiValueOperator(operator)) {
                    AttributeLookup attr = bucketRestriction.getAttr();
                    ImmutablePair<AttributeLookup, ComparisonType> key = ImmutablePair.of(attr, operator);
                    List<Object> vals = listVals.getOrDefault(key, new ArrayList<>());
                    vals.addAll(bucketRestriction.getBkt().getValues());
                    listVals.put(key, vals);
                    shouldMerge = true;
                }
            }
            if (!shouldMerge) {
                merged.add(clause);
            }
        }
        if (MapUtils.isNotEmpty(listVals)) {
            listVals.forEach((pair, vals) -> {
                List<Object> cleanVals = vals.stream().filter(Objects::nonNull).distinct().collect(Collectors.toList());
                if (CollectionUtils.isNotEmpty(cleanVals)) {
                    AttributeLookup attr = pair.getLeft();
                    ComparisonType operator = pair.getRight();
                    Bucket bkt = Bucket.valueBkt(operator, cleanVals);
                    BucketRestriction restriction = new BucketRestriction(attr, bkt);
                    merged.add(restriction);
                }
            });
        }
        return merged;
    }

}
