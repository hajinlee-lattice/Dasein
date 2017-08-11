package com.latticeengines.apps.cdl.util;

import java.util.Collections;

import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.pls.VdbSpecMetadata;

public class VdbMetadataUtilsUnitTestNG {

    @Test(groups = "unit")
    public void testBitFundamentalType() {
        VdbSpecMetadata metadata = new VdbSpecMetadata();
        metadata.setColumnName("HG_CLOUD_INFRASTRUCTURE_COMPUTI_EB4A8EFFF7");
        metadata.setDisplayName("HG_CLOUD_INFRASTRUCTURE_COMPUTI_EB4A8EFFF7");
        metadata.setKeyColumn(false);
        metadata.setDescription("");
        metadata.setDataSource(Collections.singletonList("HGData_Source"));
        metadata.setFundamentalType("Bit");
        metadata.setMostRecentUpdateDate("3/24/2017 1:36:00 AM +00:00");
        metadata.setLastTimeSourceUpdated(Collections.singletonList("3/24/2017 1:36:00 AM +00:00"));
        metadata.setTags(Collections.singletonList("External"));

        Attribute attribute = VdbMetadataUtils.convertToAttribute(metadata);
        runBasicVerification(attribute);
        Assert.assertEquals(FundamentalType.fromName(attribute.getFundamentalType()), FundamentalType.BOOLEAN);
    }

    @Test(groups = "unit")
    public void testUnknownFundamentalType() {
        VdbSpecMetadata metadata = new VdbSpecMetadata();
        metadata.setColumnName("HG_CLOUD_INFRASTRUCTURE_COMPUTI_EB4A8EFFF7");
        metadata.setDisplayName("HG_CLOUD_INFRASTRUCTURE_COMPUTI_EB4A8EFFF7");
        metadata.setKeyColumn(false);
        metadata.setDescription("");
        metadata.setDataSource(Collections.singletonList("HGData_Source"));
        metadata.setFundamentalType("Unknown");
        metadata.setMostRecentUpdateDate("3/24/2017 1:36:00 AM +00:00");
        metadata.setLastTimeSourceUpdated(Collections.singletonList("3/24/2017 1:36:00 AM +00:00"));
        metadata.setTags(Collections.singletonList("External"));

        Attribute attribute = VdbMetadataUtils.convertToAttribute(metadata);
        runBasicVerification(attribute);
        Assert.assertNull(attribute.getFundamentalType());
    }


    private void runBasicVerification(Attribute attribute) {
        Assert.assertNotNull(attribute);
        Assert.assertTrue(StringUtils.isNotBlank(attribute.getName()));
        Assert.assertTrue(StringUtils.isNotBlank(attribute.getDisplayName()));
    }
}
