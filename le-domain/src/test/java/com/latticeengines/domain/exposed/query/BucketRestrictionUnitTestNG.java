package com.latticeengines.domain.exposed.query;

import java.util.Arrays;
import java.util.Collections;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.util.RestrictionUtils;

public class BucketRestrictionUnitTestNG {

    @Test(groups = "unit")
    public void testSerDe() {
        BucketRestriction bucketSelection = new BucketRestriction(new AttributeLookup(BusinessEntity.Account, "A"),
                Bucket.rangeBkt(2, 3));
        String serialized = JsonUtils.serialize(bucketSelection);
        BucketRestriction deserialized = JsonUtils.deserialize(serialized, BucketRestriction.class);
        Assert.assertNull(deserialized.getBkt().getLabel());
        Assert.assertEquals(deserialized.getBkt().getValues().get(0), 2);
        Assert.assertEquals(deserialized.getBkt().getValues().get(1), 3);

        bucketSelection = new BucketRestriction(new AttributeLookup(BusinessEntity.Account, "A"), Bucket.nullBkt());
        serialized = JsonUtils.serialize(bucketSelection);
        deserialized = JsonUtils.deserialize(serialized, BucketRestriction.class);
        Assert.assertNotNull(deserialized);
        Assert.assertNotNull(deserialized.getBkt());
        Assert.assertNull(deserialized.getBkt().getLabel());

        bucketSelection = new BucketRestriction(new AttributeLookup(BusinessEntity.Account, "A"),
                Bucket.valueBkt("Yes"));
        serialized = JsonUtils.serialize(bucketSelection);
        deserialized = JsonUtils.deserialize(serialized, BucketRestriction.class);
        Assert.assertNotNull(deserialized);
        Assert.assertNotNull(deserialized.getBkt());
        Assert.assertEquals(deserialized.getBkt().getLabel(), "Yes");
    }

    @Test(groups = "unit")
    public void testConvertIsNull() {
        Bucket isNull = Bucket.valueBkt(ComparisonType.IS_NULL, null);
        BucketRestriction bucketSelection = new BucketRestriction(new AttributeLookup(BusinessEntity.Account, "A"),
                isNull);
        String serialized = JsonUtils.serialize(bucketSelection);
        BucketRestriction deserialized = JsonUtils.deserialize(serialized, BucketRestriction.class);
        Assert.assertNotNull(deserialized);
        Assert.assertNotNull(deserialized.getBkt());
        Assert.assertEquals(ComparisonType.IS_NULL, deserialized.getBkt().getComparisonType());
        Assert.assertNull(deserialized.getBkt().getValues());

        Restriction convertedRestriction = RestrictionUtils.convertBucketRestriction(deserialized, true);
        Assert.assertNotNull(convertedRestriction);
        Assert.assertTrue(convertedRestriction instanceof ConcreteRestriction);
        ConcreteRestriction concreteRestriction = (ConcreteRestriction) convertedRestriction;
        Assert.assertEquals(ComparisonType.IS_NULL, concreteRestriction.getRelation());
    }

    @Test(groups = "unit")
    public void testConvertIsNotNull() {
        Bucket isNotNull = Bucket.valueBkt(ComparisonType.IS_NOT_NULL, null);
        BucketRestriction bucketSelection = new BucketRestriction(new AttributeLookup(BusinessEntity.Account, "A"),
                isNotNull);
        String serialized = JsonUtils.serialize(bucketSelection);
        BucketRestriction deserialized = JsonUtils.deserialize(serialized, BucketRestriction.class);
        Assert.assertEquals(ComparisonType.IS_NOT_NULL, deserialized.getBkt().getComparisonType());
        Assert.assertNull(deserialized.getBkt().getValues());

        Restriction convertedRestriction = RestrictionUtils.convertBucketRestriction(deserialized, true);
        Assert.assertNotNull(convertedRestriction);
        Assert.assertTrue(convertedRestriction instanceof ConcreteRestriction);
        ConcreteRestriction concreteRestriction = (ConcreteRestriction) convertedRestriction;
        Assert.assertEquals(ComparisonType.IS_NOT_NULL, concreteRestriction.getRelation());
    }

    @Test(groups = "unit")
    public void testConvertEqual() {
        Bucket eq2 = Bucket.valueBkt(ComparisonType.EQUAL, Collections.singletonList(2));
        BucketRestriction bucketSelection = new BucketRestriction(new AttributeLookup(BusinessEntity.Account, "A"),
                eq2);
        String serialized = JsonUtils.serialize(bucketSelection);
        BucketRestriction deserialized = JsonUtils.deserialize(serialized, BucketRestriction.class);
        Assert.assertNotNull(deserialized);
        Assert.assertNotNull(deserialized.getBkt());
        Assert.assertEquals(ComparisonType.EQUAL, deserialized.getBkt().getComparisonType());
        Assert.assertEquals(1, deserialized.getBkt().getValues().size());
        Assert.assertEquals(2, deserialized.getBkt().getValues().get(0));

        Restriction convertedRestriction = RestrictionUtils.convertBucketRestriction(deserialized, true);
        Assert.assertNotNull(convertedRestriction);
        Assert.assertTrue(convertedRestriction instanceof ConcreteRestriction);
        ConcreteRestriction concreteRestriction = (ConcreteRestriction) convertedRestriction;
        Assert.assertEquals(ComparisonType.EQUAL, concreteRestriction.getRelation());
    }

    @Test(groups = "unit")
    public void testConvertNotEqual() {
        Bucket isNotEqual = Bucket.valueBkt(ComparisonType.NOT_EQUAL, Arrays.asList(1));
        BucketRestriction bucketRestriction = new BucketRestriction(new AttributeLookup(BusinessEntity.Account, "A"),
                isNotEqual);
        Restriction convertedRestriction = RestrictionUtils.convertBucketRestriction(bucketRestriction, true);
        Assert.assertNotNull(convertedRestriction);
        Assert.assertTrue(convertedRestriction instanceof ConcreteRestriction);
        ConcreteRestriction concreteRestriction = (ConcreteRestriction) convertedRestriction;
        Assert.assertEquals(ComparisonType.NOT_EQUAL, concreteRestriction.getRelation());
        Assert.assertFalse(concreteRestriction.getNegate());
    }

    @Test(groups = "unit")
    public void testConvertInCollection() {
        Bucket inCollection = Bucket.valueBkt(ComparisonType.IN_COLLECTION, Arrays.asList(1, 3, 5));
        BucketRestriction bucketSelection = new BucketRestriction(new AttributeLookup(BusinessEntity.Account, "A"),
                inCollection);
        String serialized = JsonUtils.serialize(bucketSelection);
        BucketRestriction deserialized = JsonUtils.deserialize(serialized, BucketRestriction.class);
        Assert.assertNotNull(deserialized);
        Assert.assertNotNull(deserialized.getBkt());
        Assert.assertEquals(ComparisonType.IN_COLLECTION, deserialized.getBkt().getComparisonType());
        Assert.assertEquals(3, deserialized.getBkt().getValues().size());
        Assert.assertEquals(1, deserialized.getBkt().getValues().get(0));
        Assert.assertEquals(3, deserialized.getBkt().getValues().get(1));
        Assert.assertEquals(5, deserialized.getBkt().getValues().get(2));

        Restriction convertedRestriction = RestrictionUtils.convertBucketRestriction(deserialized, true);
        Assert.assertNotNull(convertedRestriction);
        Assert.assertTrue(convertedRestriction instanceof ConcreteRestriction);
        ConcreteRestriction concreteRestriction = (ConcreteRestriction) convertedRestriction;
        Assert.assertEquals(ComparisonType.IN_COLLECTION, concreteRestriction.getRelation());
    }

    @Test(groups = "unit")
    public void testHasEverPurchased() {
        TimeFilter ever = TimeFilter.ever();
        Bucket hasPurchased = Bucket.txnBkt(new Bucket.Transaction("Product1", ever, null, null, false));
        BucketRestriction bucketRestriction = new BucketRestriction(
                new AttributeLookup(BusinessEntity.PurchaseHistory, "AnyCol"), hasPurchased);
        String serialized = JsonUtils.serialize(bucketRestriction);
        BucketRestriction deserialized = JsonUtils.deserialize(serialized, BucketRestriction.class);
        Assert.assertNotNull(deserialized);
        Assert.assertNotNull(deserialized.getBkt());
        Assert.assertNull(deserialized.getBkt().getComparisonType());
        Assert.assertNull(deserialized.getBkt().getValues());

        Bucket.Transaction deserializedTxn = deserialized.getBkt().getTransaction();
        Assert.assertNotNull(deserializedTxn);
        Assert.assertEquals(deserializedTxn.getTimeFilter().getRelation(), ComparisonType.EVER);

        Restriction convertedRestriction = RestrictionUtils.convertBucketRestriction(deserialized, true);
        Assert.assertNotNull(convertedRestriction);
        Assert.assertTrue(convertedRestriction instanceof TransactionRestriction);
        TransactionRestriction transactionRestriction = (TransactionRestriction) convertedRestriction;
        Assert.assertEquals(transactionRestriction.getTimeFilter().getRelation(), ComparisonType.EVER);
        Assert.assertEquals(transactionRestriction.getProductId(), "Product1");
    }

    @Test(groups = "unit")
    public void testDateRestriction() {
        TimeFilter last = new TimeFilter(ComparisonType.LAST, Collections.singletonList(7));

        Bucket bkt = Bucket.dateBkt(last);
        BucketRestriction bucketRestriction = new BucketRestriction(new AttributeLookup(BusinessEntity.Account, "A"),
                bkt);
        String serialized = JsonUtils.serialize(bucketRestriction);
        BucketRestriction deserialized = JsonUtils.deserialize(serialized, BucketRestriction.class);
        Assert.assertNotNull(deserialized);
        Assert.assertNotNull(deserialized.getBkt());
        Assert.assertNull(deserialized.getBkt().getComparisonType());
        Assert.assertNull(deserialized.getBkt().getValues());

        TimeFilter dateFilter = deserialized.getBkt().getDateFilter();
        Assert.assertNotNull(dateFilter);
        Assert.assertNotNull(dateFilter.getRelation());
        Assert.assertEquals(dateFilter.getRelation(), ComparisonType.LAST);
        Assert.assertEquals(dateFilter.getValues().get(0), 7);

        Restriction convertedRestriction = RestrictionUtils.convertBucketRestriction(deserialized, false);
        Assert.assertNotNull(convertedRestriction);
        Assert.assertTrue(convertedRestriction instanceof DateRestriction);

        DateRestriction dateRestriction = (DateRestriction) convertedRestriction;
        Assert.assertEquals(dateRestriction.getAttr().getAttribute(), "A");
        Assert.assertNotNull(dateRestriction.getTimeFilter());
        Assert.assertEquals(dateRestriction.getTimeFilter().getRelation(), ComparisonType.LAST);
        Assert.assertEquals(dateRestriction.getTimeFilter().getValues().get(0), 7);
    }

    @Test(groups = "unit")
    public void testTransactionBucket() {
        // last quarter
        TimeFilter lastQuarter = new TimeFilter(ComparisonType.EQUAL, PeriodStrategy.Template.Quarter.name(),
                Collections.singletonList(1));
        // amount > 1000
        AggregationFilter amtFilter = new AggregationFilter(ComparisonType.GREATER_THAN,
                Collections.singletonList(1000));
        // quantity <= 10
        AggregationFilter qtyFilter = new AggregationFilter(ComparisonType.LESS_OR_EQUAL,
                Collections.singletonList(10));

        Bucket bkt = Bucket.txnBkt(new Bucket.Transaction("Product1", lastQuarter, amtFilter, qtyFilter, null));
        BucketRestriction bucketRestriction = new BucketRestriction(
                new AttributeLookup(BusinessEntity.PurchaseHistory, "AnyCol"), bkt);
        String serialized = JsonUtils.serialize(bucketRestriction);
        BucketRestriction deserialized = JsonUtils.deserialize(serialized, BucketRestriction.class);
        Assert.assertNotNull(deserialized);
        Assert.assertNotNull(deserialized.getBkt());
        Assert.assertNull(deserialized.getBkt().getComparisonType());
        Assert.assertNull(deserialized.getBkt().getValues());

        Bucket.Transaction deserializedTxn = deserialized.getBkt().getTransaction();
        Assert.assertNotNull(deserializedTxn);
        Assert.assertNotNull(deserializedTxn.getTimeFilter());
        Assert.assertEquals(deserializedTxn.getTimeFilter().getRelation(), ComparisonType.EQUAL);
        Assert.assertEquals(deserializedTxn.getTimeFilter().getPeriod(), "Quarter");
        Assert.assertNotNull(deserializedTxn.getSpentFilter());
        Assert.assertNotNull(deserializedTxn.getUnitFilter());

        Restriction convertedRestriction = RestrictionUtils.convertBucketRestriction(deserialized, true);
        Assert.assertNotNull(convertedRestriction);
        Assert.assertTrue(convertedRestriction instanceof TransactionRestriction);

        TransactionRestriction transactionRestriction = (TransactionRestriction) convertedRestriction;
        Assert.assertEquals(transactionRestriction.getProductId(), "Product1");
        Assert.assertNotNull(transactionRestriction.getTimeFilter());
        Assert.assertEquals(transactionRestriction.getTimeFilter().getRelation(), ComparisonType.EQUAL);
        Assert.assertEquals(transactionRestriction.getTimeFilter().getPeriod(), "Quarter");
        Assert.assertNotNull(transactionRestriction.getSpentFilter());
        Assert.assertEquals(transactionRestriction.getSpentFilter().getSelector(), AggregationSelector.SPENT);
        Assert.assertNotNull(transactionRestriction.getUnitFilter());
        Assert.assertEquals(transactionRestriction.getUnitFilter().getSelector(), AggregationSelector.UNIT);
    }

}
