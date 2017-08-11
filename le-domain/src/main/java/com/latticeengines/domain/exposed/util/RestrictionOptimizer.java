package com.latticeengines.domain.exposed.util;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.LogicalOperator;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.Restriction;

public class RestrictionOptimizer {

    public static Restriction optimize(Restriction restriction) {
        if (restriction == null || restriction instanceof BucketRestriction) {
            return restriction;
        } else if (restriction instanceof LogicalRestriction) {
            return group(flatten(restriction));
        } else {
            throw new RuntimeException("Can only optimize bucket and logical restrictions");
        }
    }

    public static Restriction flatten(Restriction restriction) {
        if (restriction == null || restriction instanceof BucketRestriction) {
            return restriction;
        } else if (restriction instanceof LogicalRestriction) {
            return flattenLogicalRestriction((LogicalRestriction) restriction);
        } else {
            throw new RuntimeException("Can only flatten bucket and logical restrictions");
        }
    }

    private static Restriction flattenLogicalRestriction(LogicalRestriction logicalRestriction) {
        if (logicalRestriction.getRestrictions() == null || logicalRestriction.getRestrictions().isEmpty()) {
            return null;
        }

        List<Restriction> children = new ArrayList<>();
        logicalRestriction.getRestrictions().forEach(restriction -> {
            if (restriction != null) {
                Restriction flatRestriction;
                if (restriction instanceof LogicalRestriction) {
                    flatRestriction = flatten(restriction);
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

        logicalRestriction.setRestrictions(children);

        if (children.isEmpty()) {
            return null;
        } else if (logicalRestriction.getRestrictions().size() == 1) {
            return logicalRestriction.getRestrictions().get(0);
        } else {
            return logicalRestriction;
        }
    }

    public static Restriction group(Restriction restriction) {
        if (restriction == null || restriction instanceof BucketRestriction) {
            return restriction;
        } else if (restriction instanceof LogicalRestriction) {
            return groupLogicalRestriction((LogicalRestriction) restriction);
        } else {
            throw new RuntimeException("Can only group bucket and logical restrictions");
        }
    }

    private static Restriction groupLogicalRestriction(LogicalRestriction logicalRestriction) {
        List<BucketRestriction> bkts = new ArrayList<>();
        List<Restriction> nonBkts = new ArrayList<>();
        logicalRestriction.getRestrictions().forEach(restriction -> {
            if (restriction instanceof LogicalRestriction) {
                nonBkts.add(groupLogicalRestriction((LogicalRestriction) restriction));
            } else {
                bkts.add((BucketRestriction) restriction);
            }
        });

        Map<BusinessEntity, List<Restriction>> restrictionMap = new HashMap<>();
        for (BucketRestriction bkt : bkts) {
            BusinessEntity entity = bkt.getAttr().getEntity();
            if (!restrictionMap.containsKey(entity)) {
                restrictionMap.put(entity, new ArrayList<>());
            }
            restrictionMap.get(entity).add(bkt);
        }
        restrictionMap.entrySet().stream().sorted(Comparator.comparing(Map.Entry::getKey)) //
                .map(Map.Entry::getValue).forEach(restrictions -> {
                    if (restrictions.size() == 1) {
                        nonBkts.add(restrictions.get(0));
                    } else if (logicalRestriction.getOperator().equals(LogicalOperator.OR)) {
                        nonBkts.add(Restriction.builder().or(restrictions).build());
                    } else {
                        nonBkts.add(Restriction.builder().and(restrictions).build());
                    }
                });
        logicalRestriction.setRestrictions(nonBkts);
        return logicalRestriction;
    }

}
