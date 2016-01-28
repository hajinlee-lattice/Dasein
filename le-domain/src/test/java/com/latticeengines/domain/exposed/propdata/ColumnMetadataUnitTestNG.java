package com.latticeengines.domain.exposed.propdata;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.propdata.manage.ApprovedUsage;
import com.latticeengines.domain.exposed.propdata.manage.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ExternalColumn;
import com.latticeengines.domain.exposed.propdata.manage.FundamentalType;
import com.latticeengines.domain.exposed.propdata.manage.StatisticalType;

public class ColumnMetadataUnitTestNG {

    @Test(groups = "unit")
    public void testDeSer() throws IOException {
        ColumnMetadata cm = new ColumnMetadata();
        cm.setApprovedUsageList(Arrays.asList(ApprovedUsage.MODEL, ApprovedUsage.NONE));
        cm.setTagList(Collections.singletonList("External"));
        cm.setFundamentalType(FundamentalType.BOOLEAN);
        cm.setStatisticalType(StatisticalType.ORDINAL);

        String serializedString = JsonUtils.serialize(cm);

        ExternalColumn deserializedColumn = JsonUtils.deserialize(serializedString, ExternalColumn.class);
        Assert.assertTrue(deserializedColumn.getApprovedUsageList().contains(ApprovedUsage.MODEL));
        Assert.assertTrue(deserializedColumn.getApprovedUsageList().contains(ApprovedUsage.NONE));
        Assert.assertEquals(deserializedColumn.getStatisticalType(), StatisticalType.ORDINAL);
        Assert.assertEquals(deserializedColumn.getFundamentalType(), FundamentalType.BOOLEAN);
    }

}
