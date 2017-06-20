package com.latticeengines.domain.exposed.query;

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
    }

}
