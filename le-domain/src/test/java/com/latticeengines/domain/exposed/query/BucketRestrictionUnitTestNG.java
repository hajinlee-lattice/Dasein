package com.latticeengines.domain.exposed.query;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.query.frontend.FrontEndBucket;

public class BucketRestrictionUnitTestNG {

    @Test(groups = "unit")
    public void testSerDe() {
        BucketRestriction bucketSelection = new BucketRestriction(new AttributeLookup(BusinessEntity.Account, "A"),
                FrontEndBucket.range(2, 3));
        String serialized = JsonUtils.serialize(bucketSelection);
        BucketRestriction deserialized = JsonUtils.deserialize(serialized, BucketRestriction.class);
        Assert.assertNull(deserialized.getBkt().getValue());
        Assert.assertEquals(deserialized.getBkt().getRange().getLeft(), 2);
        Assert.assertEquals(deserialized.getBkt().getRange().getRight(), 3);

        bucketSelection = new BucketRestriction(new AttributeLookup(BusinessEntity.Account, "A"),
                FrontEndBucket.nullBkt());
        serialized = JsonUtils.serialize(bucketSelection);
        deserialized = JsonUtils.deserialize(serialized, BucketRestriction.class);
        Assert.assertNull(deserialized.getBkt().getValue());
        Assert.assertNull(deserialized.getBkt().getRange());

        bucketSelection = new BucketRestriction(new AttributeLookup(BusinessEntity.Account, "A"),
                FrontEndBucket.value("Yes"));
        serialized = JsonUtils.serialize(bucketSelection);
        deserialized = JsonUtils.deserialize(serialized, BucketRestriction.class);
        Assert.assertEquals(deserialized.getBkt().getValue(), "Yes");
        Assert.assertNull(deserialized.getBkt().getRange());
    }

}
