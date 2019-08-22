package com.latticeengines.query.exposed.translator;

import java.util.List;

import org.apache.commons.collections4.CollectionUtils;

import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.transaction.NullMetricsImputation;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.SubQuery;
import com.latticeengines.domain.exposed.util.ActivityMetricsUtils;
import com.latticeengines.domain.exposed.util.RestrictionUtils;
import com.latticeengines.domain.exposed.util.StatsCubeUtils;

public class MetricTranslator {

    public static Restriction convert(BucketRestriction bucketRestriction, boolean useDepivotedTable) {
        if (bucketRestriction.getBkt().getChange() != null) {
            Bucket bkt = StatsCubeUtils.convertChgBucketToBucket(bucketRestriction.getBkt());
            bucketRestriction.setBkt(bkt);
        }
        if (useDepivotedTable) {
            return convertToDepivotedTable(bucketRestriction);
        } else {
            return RestrictionUtils.convertBucketRestriction(bucketRestriction, false);
        }
    }

    private static Restriction convertToDepivotedTable(BucketRestriction bucketRestriction) {
        AttributeLookup attr = bucketRestriction.getAttr();
        ComparisonType comparator = bucketRestriction.getBkt().getComparisonType();
        List<Object> values = bucketRestriction.getBkt().getValues();
        Restriction majorRes = Restriction.builder() //
                .let(BusinessEntity.Account, InterfaceName.AccountId.name()) //
                .inSubquery(constructMajorSubQuery(attr, comparator, values)) //
                .build();
        boolean needImputeNulls = needImputeNulls(attr, comparator, values);
        if (needImputeNulls) {
            Restriction minorRes = Restriction.builder() //
                    .let(BusinessEntity.Account, InterfaceName.AccountId.name()) //
                    .inSubquery(constructNullImputationSubQuery()) //
                    .build();
            return Restriction.builder().or(majorRes, minorRes).build();
        } else {
            return majorRes;
        }
    }

    private static SubQuery constructMajorSubQuery(AttributeLookup attr, ComparisonType comparator,
                                                   List<Object> values) {
        String fullAttrName = attr.getAttribute();
        String metricAttr = ActivityMetricsUtils.getNameWithPeriodFromFullName(fullAttrName);
        String bundleId = ActivityMetricsUtils.getProductIdFromFullName(fullAttrName);
        AttributeLookup metricAttrLookup = new AttributeLookup(BusinessEntity.DepivotedPurchaseHistory, metricAttr);
        Restriction metricRestriction = RestrictionUtils.convertValueComparisons(metricAttrLookup, comparator, values);
        Restriction bundleIdRestriction = Restriction.builder() //
                .let(BusinessEntity.DepivotedPurchaseHistory, InterfaceName.ProductId.name()) //
                .eq(bundleId).build();
        Restriction restriction = Restriction.builder().and(bundleIdRestriction, metricRestriction).build();
        SubQuery subQuery = new SubQuery();
        Query metricQuery = Query.builder() //
                .select(new AttributeLookup(BusinessEntity.DepivotedPurchaseHistory, InterfaceName.AccountId.name())) //
                .from(BusinessEntity.DepivotedPurchaseHistory) //
                .where(restriction) //
                .build();
        subQuery.setQuery(metricQuery);
        return subQuery;
    }

    private static SubQuery constructNullImputationSubQuery() {
        Restriction isNullRestriction = Restriction.builder() //
                .let(BusinessEntity.DepivotedPurchaseHistory, InterfaceName.ProductId.name()).isNull().build();
        SubQuery subQuery = new SubQuery();
        Query metricQuery = Query.builder() //
                .select(new AttributeLookup(BusinessEntity.Account, InterfaceName.AccountId.name())) //
                .from(BusinessEntity.Account) //
                .where(isNullRestriction) //
                .build();
        subQuery.setQuery(metricQuery);
        return subQuery;
    }

    private static boolean needImputeNulls(AttributeLookup attr, ComparisonType comparator,
                                           List<Object> values) {
        String fullAttrName = attr.getAttribute();
        NullMetricsImputation imputation = ActivityMetricsUtils.getNullImputation(fullAttrName);
        // only handles zero imputation now
        return NullMetricsImputation.ZERO.equals(imputation) && containsZero(comparator, values);
    }

    private static boolean containsZero(ComparisonType comparator, List<Object> vals) {
        boolean containsZero = false;
        if (CollectionUtils.isEmpty(vals)) {
            return false;
        } else if (vals.size() == 2) {
            Double upperBound = toDouble(vals.get(1));
            Double lowerBound = toDouble(vals.get(0));
            containsZero = lowerBound * upperBound <= 0;
        } else {
            Double val = toDouble(vals.get(0));
            switch (comparator) {
                case GREATER_OR_EQUAL:
                    containsZero = val <= 0;
                    break;
                case GREATER_THAN:
                    containsZero = val < 0;
                    break;
                case LESS_OR_EQUAL:
                    containsZero = val >= 0;
                    break;
                case LESS_THAN:
                    containsZero = val > 0;
                    break;
                default:
                    break;
            }
        }
        return containsZero;
    }

    private static Double toDouble(Object val) {
        if (val == null) {
            return null;
        } else if (val instanceof Double) {
            return (Double) val;
        } else {
            return Double.valueOf(val.toString());
        }
    }

}
