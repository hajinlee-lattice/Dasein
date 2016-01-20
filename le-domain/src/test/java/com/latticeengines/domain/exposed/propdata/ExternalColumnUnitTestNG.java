package com.latticeengines.domain.exposed.propdata;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.propdata.manage.ApprovedUsage;
import com.latticeengines.domain.exposed.propdata.manage.ExternalColumn;
import com.latticeengines.domain.exposed.propdata.manage.FundamentalType;

public class ExternalColumnUnitTestNG {

    @Test(groups = "unit")
    public void testDeSer() throws IOException {
        ExternalColumn column = new ExternalColumn();
        column.setExternalColumnID("column-a");
        column.setApprovedUsageList(Arrays.asList(ApprovedUsage.MODEL, ApprovedUsage.NONE));
        column.setTagList(Collections.singletonList("External"));
        column.setFundamentalType(FundamentalType.BOOLEAN);
        column.setStatisticalType(StatisticalType.ORDINAL);

        String serializedString = JsonUtils.serialize(column);

        ExternalColumn deserializedColumn = JsonUtils.deserialize(serializedString, ExternalColumn.class);
        Assert.assertEquals(deserializedColumn.getExternalColumnID(), "column-a");
        Assert.assertTrue(deserializedColumn.getApprovedUsageList().contains(ApprovedUsage.MODEL));
        Assert.assertTrue(deserializedColumn.getApprovedUsageList().contains(ApprovedUsage.NONE));
        Assert.assertEquals(deserializedColumn.getStatisticalType(), StatisticalType.ORDINAL);
        Assert.assertEquals(deserializedColumn.getFundamentalType(), FundamentalType.BOOLEAN);
    }

}
