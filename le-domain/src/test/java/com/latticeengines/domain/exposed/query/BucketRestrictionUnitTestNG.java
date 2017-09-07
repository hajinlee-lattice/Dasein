package com.latticeengines.domain.exposed.query;

import java.util.Arrays;
import java.util.Collections;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;

public class BucketRestrictionUnitTestNG {

    @Test(groups = "unit")
    public void testSerDe() {
        BucketRestriction bucketSelection = new BucketRestriction(new AttributeLookup(BusinessEntity.Account, "A"),
                Bucket.rangeBkt(2, 3));
        String serialized = JsonUtils.serialize(bucketSelection);
        BucketRestriction deserialized = JsonUtils.deserialize(serialized, BucketRestriction.class);
        Assert.assertNull(deserialized.getBkt().getLabel());
        Assert.assertEquals(deserialized.getBkt().getRange().getLeft(), 2);
        Assert.assertEquals(deserialized.getBkt().getRange().getRight(), 3);

        bucketSelection = new BucketRestriction(new AttributeLookup(BusinessEntity.Account, "A"),
                Bucket.nullBkt());
        serialized = JsonUtils.serialize(bucketSelection);
        deserialized = JsonUtils.deserialize(serialized, BucketRestriction.class);
        Assert.assertNull(deserialized.getBkt().getLabel());
        Assert.assertNull(deserialized.getBkt().getRange());

        bucketSelection = new BucketRestriction(new AttributeLookup(BusinessEntity.Account, "A"),
                Bucket.valueBkt("Yes"));
        serialized = JsonUtils.serialize(bucketSelection);
        deserialized = JsonUtils.deserialize(serialized, BucketRestriction.class);
        Assert.assertEquals(deserialized.getBkt().getLabel(), "Yes");
        Assert.assertNull(deserialized.getBkt().getRange());

        Bucket eq2 = Bucket.valueBkt(ComparisonType.EQUAL, Collections.singletonList(2));
        bucketSelection = new BucketRestriction(new AttributeLookup(BusinessEntity.Account, "A"), eq2);
        serialized = JsonUtils.serialize(bucketSelection);
        deserialized = JsonUtils.deserialize(serialized, BucketRestriction.class);
        Assert.assertEquals(ComparisonType.EQUAL, deserialized.getBkt().getComparisonType());
        Assert.assertEquals(1, deserialized.getBkt().getValues().size());
        Assert.assertEquals(2, deserialized.getBkt().getValues().get(0));

        Bucket isNull = Bucket.valueBkt(ComparisonType.IS_NULL, null);
        bucketSelection = new BucketRestriction(new AttributeLookup(BusinessEntity.Account, "A"), isNull);
        serialized = JsonUtils.serialize(bucketSelection);
        deserialized = JsonUtils.deserialize(serialized, BucketRestriction.class);
        Assert.assertEquals(ComparisonType.IS_NULL, deserialized.getBkt().getComparisonType());
        Assert.assertNull(deserialized.getBkt().getValues());

        Bucket isNotNull = Bucket.valueBkt(ComparisonType.IS_NOT_NULL, null);
        bucketSelection = new BucketRestriction(new AttributeLookup(BusinessEntity.Account, "A"), isNotNull);
        serialized = JsonUtils.serialize(bucketSelection);
        deserialized = JsonUtils.deserialize(serialized, BucketRestriction.class);
        Assert.assertEquals(ComparisonType.IS_NOT_NULL, deserialized.getBkt().getComparisonType());
        Assert.assertNull(deserialized.getBkt().getValues());

        Bucket inCollection = Bucket.valueBkt(ComparisonType.IN_COLLECTION, Arrays.asList(1, 3, 5));
        bucketSelection = new BucketRestriction(new AttributeLookup(BusinessEntity.Account, "A"), inCollection);
        serialized = JsonUtils.serialize(bucketSelection);
        deserialized = JsonUtils.deserialize(serialized, BucketRestriction.class);
        Assert.assertEquals(ComparisonType.IN_COLLECTION, deserialized.getBkt().getComparisonType());
        Assert.assertEquals(3, deserialized.getBkt().getValues().size());
        Assert.assertEquals(1, deserialized.getBkt().getValues().get(0));
        Assert.assertEquals(3, deserialized.getBkt().getValues().get(1));
        Assert.assertEquals(5, deserialized.getBkt().getValues().get(2));
    }

    @Test(groups = "unit")
    public void testConvertIsNull() {
        Bucket isNull = Bucket.valueBkt(ComparisonType.IS_NULL, null);
        BucketRestriction bucketRestriction = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Account, "A"), isNull);
        Restriction convertedRestriction = bucketRestriction.convert();
        Assert.assertNotNull(convertedRestriction);
        Assert.assertTrue(convertedRestriction instanceof ConcreteRestriction);
        ConcreteRestriction concreteRestriction = (ConcreteRestriction) convertedRestriction;
        Assert.assertEquals(ComparisonType.IS_NULL, concreteRestriction.getRelation());
    }

    @Test(groups = "unit")
    public void testConvertIsNotNull() {
        Bucket isNull = Bucket.valueBkt(ComparisonType.IS_NOT_NULL, null);
        BucketRestriction bucketRestriction = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Account, "A"), isNull);
        Restriction convertedRestriction = bucketRestriction.convert();
        Assert.assertNotNull(convertedRestriction);
        Assert.assertTrue(convertedRestriction instanceof ConcreteRestriction);
        ConcreteRestriction concreteRestriction = (ConcreteRestriction) convertedRestriction;
        Assert.assertEquals(ComparisonType.IS_NOT_NULL, concreteRestriction.getRelation());
    }

    @Test(groups = "unit")
    public void testConvertEqual() {
        Bucket isEqual = Bucket.valueBkt(ComparisonType.EQUAL, Arrays.asList(1));
        BucketRestriction bucketRestriction = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Account, "A"), isEqual);
        Restriction convertedRestriction = bucketRestriction.convert();
        Assert.assertNotNull(convertedRestriction);
        Assert.assertTrue(convertedRestriction instanceof ConcreteRestriction);
        ConcreteRestriction concreteRestriction = (ConcreteRestriction) convertedRestriction;
        Assert.assertEquals(ComparisonType.EQUAL, concreteRestriction.getRelation());
    }

    @Test(groups = "unit")
    public void testConvertNotEqual() {
        Bucket isNotEqual = Bucket.valueBkt(ComparisonType.NOT_EQUAL, Arrays.asList(1));
        BucketRestriction bucketRestriction = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Account, "A"), isNotEqual);
        Restriction convertedRestriction = bucketRestriction.convert();
        Assert.assertNotNull(convertedRestriction);
        Assert.assertTrue(convertedRestriction instanceof ConcreteRestriction);
        ConcreteRestriction concreteRestriction = (ConcreteRestriction) convertedRestriction;
        Assert.assertEquals(ComparisonType.EQUAL, concreteRestriction.getRelation());
        Assert.assertTrue(concreteRestriction.getNegate());
    }

    @Test(groups = "unit")
    public void testConvertInCollection() {
        Bucket inCollection = Bucket.valueBkt(ComparisonType.IN_COLLECTION, Arrays.asList(1, 3, 5));
        BucketRestriction bucketRestriction = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Account, "A"), inCollection);
        Restriction convertedRestriction = bucketRestriction.convert();
        Assert.assertNotNull(convertedRestriction);
        Assert.assertTrue(convertedRestriction instanceof ConcreteRestriction);
        ConcreteRestriction concreteRestriction = (ConcreteRestriction) convertedRestriction;
        Assert.assertEquals(ComparisonType.IN_COLLECTION, concreteRestriction.getRelation());
    }

}
