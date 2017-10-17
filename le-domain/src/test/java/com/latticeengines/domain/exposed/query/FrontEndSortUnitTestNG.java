package com.latticeengines.domain.exposed.query;

import java.util.Arrays;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.query.frontend.FrontEndSort;

public class FrontEndSortUnitTestNG {

    @Test(groups = "unit")
    public void testSerDe() {
        FrontEndSort sort = new FrontEndSort(Arrays.asList( //
                new AttributeLookup(BusinessEntity.Account, "A"), //
                new AttributeLookup(BusinessEntity.Account, "B")), false);
        String serialized = JsonUtils.serialize(sort);
        FrontEndSort deserialized = JsonUtils.deserialize(serialized, FrontEndSort.class);
        Assert.assertFalse(Boolean.TRUE.equals(deserialized.getDescending()));
        Assert.assertNull(deserialized.getDescending());
    }
}
