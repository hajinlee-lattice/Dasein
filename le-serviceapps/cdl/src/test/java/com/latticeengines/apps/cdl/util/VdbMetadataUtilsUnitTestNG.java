package com.latticeengines.apps.cdl.util;

import java.util.Collections;

import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.StatisticalType;
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

    @Test(groups = "unit", dataProvider = "statisticalTypeTestData")
    public void testStatisticalType(String vdbType, StatisticalType expected) {
        VdbSpecMetadata metadata = createVdbMetadata();
        metadata.setStatisticalType(vdbType);

        Attribute attribute = VdbMetadataUtils.convertToAttribute(metadata);
        runBasicVerification(attribute);
        if (expected == null) {
            Assert.assertNull(attribute.getStatisticalType());
            if (!StringUtils.isBlank(vdbType)) {
                Assert.assertNotNull(attribute.getApprovedUsage());
                boolean noneApprovedUsage = false;
                for (String approvedUsage : attribute.getApprovedUsage()) {
                    if (approvedUsage.equalsIgnoreCase(ApprovedUsage.NONE.getName())) {
                        noneApprovedUsage = true;
                        break;
                    }
                }
                Assert.assertTrue(noneApprovedUsage);
            }
        } else  {
            Assert.assertEquals(StatisticalType.fromName(attribute.getStatisticalType()), expected);
        }
    }

    @DataProvider(name = "statisticalTypeTestData", parallel = true)
    public Object[][] provideStatisticalTypeTestData() {
        return new Object[][]{
                { "",  null },
                { null,  null },
                { "Dummy Type", null },
                { "interval",  StatisticalType.INTERVAL },
                { "INTERVAL",  StatisticalType.INTERVAL }
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

    @Test(groups = "unit", dataProvider = "acceptableTestData")
    public void testIsAcceptableDataType(String srcType, String destType, boolean expected) {
        boolean result = VdbMetadataUtils.isAcceptableDataType(srcType, destType);
        Assert.assertEquals(result, expected);
    }

    @DataProvider(name = "acceptableTestData")
    public Object[][] acceptableTestDataProvider() {
        return new Object[][]{
                {"int", "Long", true},
                {"long", "Int", true},
                {"Float", "double", true},
                {"Double", "float", true},
                {"string", "VARCHAR(100)", true},
                {"nvarchar(max)", "string", true},
                {"varchar(10)", "NVARCHAR(200)", true},
                {"Nvarchar(max)", "varchar(max)", true},
                {"int", "string", false},
                {"float", "VARCHAR(100)", false},
                {"bit", "int", false},
                {"Double", "BYTE", false},
                {"short", "dateTime", false},
        };
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
