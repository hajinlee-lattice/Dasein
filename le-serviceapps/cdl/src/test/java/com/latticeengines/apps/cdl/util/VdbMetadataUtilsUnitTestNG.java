package com.latticeengines.apps.cdl.util;

import java.util.Arrays;
import java.util.Collections;

import org.apache.avro.Schema;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.StatisticalType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.domain.exposed.pls.VdbMetadataExtension;
import com.latticeengines.domain.exposed.pls.VdbSpecMetadata;

public class VdbMetadataUtilsUnitTestNG {

    @Test(groups = "unit", dataProvider = "fundamentalTypeTestData")
    public void testFundamentalType(String vdbValue, FundamentalType expected) {
        VdbSpecMetadata metadata = createVdbMetadata();
        metadata.setFundamentalType(vdbValue);

        Attribute attribute = VdbMetadataUtils.convertToAttribute(metadata, "Account");
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

        Attribute attribute = VdbMetadataUtils.convertToAttribute(metadata, "Account");
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
            Attribute attribute = VdbMetadataUtils.convertToAttribute(metadata, "Account");
            runBasicVerification(attribute);
            Assert.assertNull(attribute.getFundamentalType());
        }
    }

    @Test(groups = "unit")
    public void testValidateVdbTable() {
        Table vdbTable = new Table();
        vdbTable.setName("VdbTable");
        Attribute a1 = new Attribute();
        a1.setName(AvroUtils.getAvroFriendlyString("Attr_1"));
        a1.setDisplayName(a1.getName());
        a1.setSourceLogicalDataType("varchar(100)");
        vdbTable.addAttribute(a1);

        Attribute a2 = new Attribute();
        a2.setName(AvroUtils.getAvroFriendlyString("Attr_2"));
        a2.setDisplayName(a2.getName());
        a2.setSourceLogicalDataType("nvarchar(100)");
        vdbTable.addAttribute(a2);

        Assert.assertTrue(VdbMetadataUtils.validateVdbTable(vdbTable));

        Attribute a3 = new Attribute();
        a3.setName(AvroUtils.getAvroFriendlyString("attr_2"));
        a3.setDisplayName(a3.getName());
        a3.setSourceLogicalDataType("nvarchar(100)");
        vdbTable.addAttribute(a3);
        vdbTable.addAttribute(a3);

        Assert.assertFalse(VdbMetadataUtils.validateVdbTable(vdbTable));

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
                {"NVarChar(256)", "NVarChar(100)", true},
                {"NVarChar(1900)", "Nvarchar(3800)", true},
                {"int", "string", false},
                {"float", "VARCHAR(100)", false},
                {"bit", "int", false},
                {"Double", "BYTE", false},
                {"short", "dateTime", false},
                {"Date", "Date", true},
                {"Date", "DateTime", true},
                {"DateTime", "Date", true},
                {"datetime", "datetime", true},
                {"Date", "time", false},
        };
    }

    @Test(groups = "unit", dataProvider = "typeConvertData")
    public void testTypeConvert(String vdbType, String avroType) {
        String convertType = VdbMetadataUtils.getAvroTypeFromDataType(vdbType);
        Assert.assertEquals(convertType, avroType);
    }

    @DataProvider(name = "typeConvertData")
    public Object[][] typeConvertDataProvider() {
        return new Object[][]{
                {"int", Schema.Type.INT.name()},
                {"long", Schema.Type.LONG.name()},
                {"Float", Schema.Type.FLOAT.name()},
                {"Double", Schema.Type.DOUBLE.name()},
                {"string", Schema.Type.STRING.name()},
                {"nvarchar(max)", Schema.Type.STRING.name()},
                {"varchar(10)", Schema.Type.STRING.name()},
                {"Nvarchar(max)", Schema.Type.STRING.name()},
                {"NVarChar(256)", Schema.Type.STRING.name()},
                {"NVarChar(1900)", Schema.Type.STRING.name()},
                {"float", Schema.Type.FLOAT.name()},
                {"bit", Schema.Type.BOOLEAN.name()},
                {"short", Schema.Type.INT.name()},
                {"Date", Schema.Type.LONG.name()},
                {"DateTimeOffset",  Schema.Type.LONG.name()},
                {"DateTime",  Schema.Type.LONG.name()},
                {"datetime",  Schema.Type.LONG.name()},
        };
    }


    @Test(groups = "unit")
    public void testMetadataExtension() {
        VdbSpecMetadata metadata = createVdbMetadata();
        Attribute attribute = VdbMetadataUtils.convertToAttribute(metadata, "Account");
        Assert.assertNotNull(attribute);
        Assert.assertTrue(attribute.getExcludeFromAll());
        Assert.assertFalse(attribute.getExcludeFromListView());
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
        metadata.setTags(Arrays.asList("External", "ExcludeFromAll"));
        metadata.setDataType("Nvarchar(100)");
        VdbMetadataExtension vdbMetadataExtension = new VdbMetadataExtension();
        vdbMetadataExtension.setKey("ExcludeFromAll");
        vdbMetadataExtension.setValue("true");
        metadata.setExtensions(Collections.singletonList(vdbMetadataExtension));
        return metadata;
    }

    private void runBasicVerification(Attribute attribute) {
        Assert.assertNotNull(attribute);
        Assert.assertTrue(StringUtils.isNotBlank(attribute.getName()));
        Assert.assertTrue(StringUtils.isNotBlank(attribute.getDisplayName()));
        Assert.assertTrue(CollectionUtils.isNotEmpty(attribute.getTags()));
        Assert.assertEquals(Tag.INTERNAL.getName(), attribute.getTags().get(0));
        Assert.assertEquals(attribute.getPhysicalDataType(), Schema.Type.STRING.name());
    }
}
