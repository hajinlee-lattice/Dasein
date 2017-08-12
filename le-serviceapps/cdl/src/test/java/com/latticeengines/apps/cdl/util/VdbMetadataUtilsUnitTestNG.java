package com.latticeengines.apps.cdl.util;

import java.util.Collections;

import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.pls.VdbSpecMetadata;

public class VdbMetadataUtilsUnitTestNG {

    @Test(groups = "unit", dataProvider = "fundamentalTypeTestData")
    public void testFundamentalType(String vdbValue, FundamentalType expected) {
        VdbSpecMetadata metadata = createVdbMetadata();
        metadata.setFundamentalType(vdbValue);

        Attribute attribute = VdbMetadataUtils.convertToAttribute(metadata);
        runBasicVerification(attribute);
        if (expected == null) {
            Assert.assertNull(attribute.getFundamentalType());
        } else {
            Assert.assertEquals(FundamentalType.fromName(attribute.getFundamentalType()), expected);
        }
    }

    @DataProvider(name = "fundamentalTypeTestData", parallel = true)
    public Object[][] provideFundamentalTypeTestData() {
        return new Object[][]{
                { "",  null },
                { null,  null },
                { "Bit",  FundamentalType.BOOLEAN },
                { "BIT",  FundamentalType.BOOLEAN },
                { "EpochTime",  FundamentalType.DATE }
        };
    }

    @Test(groups = "unit")
    public void testUnparsableFundamentalType() {
        for (String vdbValue: VdbMetadataUtils.unparsableFundamentalTypes) {
            VdbSpecMetadata metadata = createVdbMetadata();
            metadata.setFundamentalType(vdbValue);
            Attribute attribute = VdbMetadataUtils.convertToAttribute(metadata);
            runBasicVerification(attribute);
            Assert.assertNull(attribute.getFundamentalType());
        }
    }

    private VdbSpecMetadata createVdbMetadata() {
        VdbSpecMetadata metadata = new VdbSpecMetadata();
        metadata.setColumnName("HG_CLOUD_INFRASTRUCTURE_COMPUTI_EB4A8EFFF7");
        metadata.setDisplayName("HG_CLOUD_INFRASTRUCTURE_COMPUTI_EB4A8EFFF7");
        metadata.setKeyColumn(false);
        metadata.setDescription("");
        metadata.setDataSource(Collections.singletonList("HGData_Source"));
        metadata.setMostRecentUpdateDate("3/24/2017 1:36:00 AM +00:00");
        metadata.setLastTimeSourceUpdated(Collections.singletonList("3/24/2017 1:36:00 AM +00:00"));
        metadata.setTags(Collections.singletonList("External"));
        return metadata;
    }

    private void runBasicVerification(Attribute attribute) {
        Assert.assertNotNull(attribute);
        Assert.assertTrue(StringUtils.isNotBlank(attribute.getName()));
        Assert.assertTrue(StringUtils.isNotBlank(attribute.getDisplayName()));
    }
}
