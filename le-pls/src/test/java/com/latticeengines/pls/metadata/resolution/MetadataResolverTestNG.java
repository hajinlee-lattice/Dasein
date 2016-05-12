package com.latticeengines.pls.metadata.resolution;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.avro.Schema.Type;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBaseDeprecated;

public class MetadataResolverTestNG extends PlsFunctionalTestNGBaseDeprecated {

    @Autowired
    private Configuration yarnConfiguration;

    String hdfsPath = "/tmp/test_metadata_resolution";

    @BeforeClass(groups = "functional")
    public void setup() throws IOException {
        String path = ClassLoader.getSystemResource(
                "com/latticeengines/pls/service/impl/fileuploadserviceimpl/file1.csv").getPath();

        HdfsUtils.rmdir(yarnConfiguration, hdfsPath);
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, path, hdfsPath);
    }

    @Test(groups = "functional")
    public void getUnknownColumns() {
        MetadataResolver resolver = new MetadataResolver(hdfsPath,
                SchemaInterpretation.SalesforceAccount, null, yarnConfiguration);
        resolver.calculate();

        Set<String> expectedUnknownColumns = Sets.newHashSet(new String[] { "Some Column" });
        List<ColumnTypeMapping> mappings = resolver.getUnknownColumns();
        assertEquals(mappings.size(), expectedUnknownColumns.size());
        for (ColumnTypeMapping mapping : mappings) {
            assertTrue(expectedUnknownColumns.contains(mapping.getColumnName()));
            assertEquals(mapping.getColumnType(), Type.STRING.name());
        }

        resolver = new MetadataResolver(hdfsPath, SchemaInterpretation.SalesforceAccount,
                mappings, yarnConfiguration);
        resolver.calculate();
        Table table = resolver.getMetadata();

        assertEquals(table.getAttribute(InterfaceName.Id).getDisplayName(), "Account ID");
        assertEquals(table.getAttribute(InterfaceName.Website).getDisplayName(), "Website");
        assertEquals(table.getAttribute(InterfaceName.Event).getDisplayName(), "Event");
        assertEquals(table.getAttribute(InterfaceName.Country).getDisplayName(), "Billing Country");
        assertEquals(table.getAttribute(InterfaceName.CompanyName).getDisplayName(), "Account Name");
        assertEquals(table.getAttribute(InterfaceName.LastModifiedDate).getDisplayName(), "Last Modified Date");
        assertNull(table.getAttribute(InterfaceName.AnnualRevenue));
        Attribute attribute = table.getAttribute("Some_Column");
        assertEquals(attribute.getTags().size(), 1);
        assertEquals(attribute.getTags().get(0), ModelingMetadata.INTERNAL_TAG);
        assertEquals(attribute.getCategory(), ModelingMetadata.CATEGORY_LEAD_INFORMATION);
        assertEquals(attribute.getFundamentalType(), ModelingMetadata.FT_ALPHA);
        assertEquals(attribute.getStatisticalType(), ModelingMetadata.NOMINAL_STAT_TYPE);
        for (Attribute a : table.getAttributes()) {
            assertNotEquals(a.getTags(), 0);
            assertEquals(a.getTags().get(0), ModelingMetadata.INTERNAL_TAG);
            assertEquals(attribute.getCategory(), ModelingMetadata.CATEGORY_LEAD_INFORMATION);
        }

        ColumnTypeMapping additionalCol1 = new ColumnTypeMapping();
        additionalCol1.setColumnName("additionalCol1");
        additionalCol1.setColumnType("boolean");
        ColumnTypeMapping additionalCol2 = new ColumnTypeMapping();
        additionalCol2.setColumnName("additionalCol2");
        additionalCol2.setColumnType("Double");
        mappings.add(additionalCol1);
        mappings.add(additionalCol2);
        resolver = new MetadataResolver(hdfsPath, SchemaInterpretation.SalesforceAccount,
                mappings, yarnConfiguration);
        resolver.calculate();
        table = resolver.getMetadata();
        attribute = table.getAttribute("additionalCol1");
        assertEquals(attribute.getFundamentalType(), ModelingMetadata.FT_BOOLEAN);
        assertEquals(attribute.getStatisticalType(), ModelingMetadata.NOMINAL_STAT_TYPE);
        attribute = table.getAttribute("additionalCol2");
        assertEquals(attribute.getFundamentalType(), ModelingMetadata.FT_NUMERIC);
        assertEquals(attribute.getStatisticalType(), ModelingMetadata.RATIO_STAT_TYPE);

    }

    @Test(groups = "functional")
    public void testDuplicateHeadersFail() {

    }

    @AfterClass(groups = "functional")
    public void cleanup() throws IOException {
        HdfsUtils.rmdir(yarnConfiguration, hdfsPath);
    }
}
