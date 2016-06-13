package com.latticeengines.pls.metadata.resolution;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.avro.Schema.Type;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBaseDeprecated;
import com.latticeengines.pls.metadata.standardschemas.SchemaRepository;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.transform.v2_0_25.common.JsonUtils;

public class MetadataResolverTestNG extends PlsFunctionalTestNGBaseDeprecated {
    
    private static Logger log = Logger.getLogger(MetadataResolverTestNG.class);

    @Autowired
    private Configuration yarnConfiguration;

    String hdfsPath = "/tmp/test_metadata_resolution";

    String hdfsPath2 = "/tmp/test_metadata_resolution2";

    @Autowired
    private MetadataProxy metadataProxy;

    @BeforeClass(groups = "functional")
    public void setup() throws IOException {
        String path = ClassLoader.getSystemResource(
                "com/latticeengines/pls/service/impl/fileuploadserviceimpl/file1.csv").getPath();

        HdfsUtils.rmdir(yarnConfiguration, hdfsPath);
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, path, hdfsPath);

        path = ClassLoader.getSystemResource(
                "com/latticeengines/pls/end2end/selfServiceModeling/csvfiles/Hootsuite_PLS132_LP3_ScoringLead_20160330_165806_modified.csv").getPath();

        HdfsUtils.rmdir(yarnConfiguration, hdfsPath2);
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, path, hdfsPath2);
    }

    @Test(groups = "functional")
    public void getUnknownColumns() {
        MetadataResolver resolver = new MetadataResolver(hdfsPath, SchemaInterpretation.SalesforceAccount, null,
                yarnConfiguration);
        resolver.calculate();

        Set<String> expectedUnknownColumns = Sets.newHashSet(new String[] { "Some Column" });
        List<ColumnTypeMapping> mappings = resolver.getUnknownColumns();
        assertEquals(mappings.size(), expectedUnknownColumns.size());
        for (ColumnTypeMapping mapping : mappings) {
            assertTrue(expectedUnknownColumns.contains(mapping.getColumnName()));
            assertEquals(mapping.getColumnType(), Type.STRING.name());
        }

        resolver = new MetadataResolver(hdfsPath, SchemaInterpretation.SalesforceAccount, mappings, yarnConfiguration);
        resolver.calculate();
        Table table = resolver.getMetadata();

        assertEquals(table.getAttribute(InterfaceName.Id).getDisplayName(), "Account iD");
        assertEquals(table.getAttribute(InterfaceName.Website).getDisplayName(), "website");
        assertEquals(table.getAttribute(InterfaceName.Event).getDisplayName(), "Event");
        assertEquals(table.getAttribute(InterfaceName.Country).getDisplayName(), "Billing Country");
        assertEquals(table.getAttribute(InterfaceName.CompanyName).getDisplayName(), "ACCOUNT Name");
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
        resolver = new MetadataResolver(hdfsPath, SchemaInterpretation.SalesforceAccount, mappings, yarnConfiguration);
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
    public void testcalculateBasedOnExistingMetadata() throws IOException {
        String path = ClassLoader.getSystemResource(
                "com/latticeengines/pls/service/impl/fileuploadserviceimpl/table.json").getPath();

        Table table = JsonUtils.deserialize(FileUtils.readFileToString(new File(path)), Table.class);
        table.getAttributeFromDisplayName("Some Column").setApprovedUsage(ApprovedUsage.NONE.toString());

        final Table schema = SchemaRepository.instance().getSchema(SchemaInterpretation.SalesforceLead);
        Iterables.removeIf(table.getAttributes(), new Predicate<Attribute>() {
            @Override
            public boolean apply(@Nullable Attribute attr) {
                List<String> approvedUsages = attr.getApprovedUsage();
                List<String> tags = attr.getTags();

                if (schema.getAttribute(attr.getName()) == null
                        && (approvedUsages == null || approvedUsages.isEmpty() || approvedUsages.get(0).equals(
                                ApprovedUsage.NONE.toString())) //
                        || (tags == null || tags.isEmpty() || !tags.get(0).equals(Tag.INTERNAL.toString()))) {
                    log.info("Removing attr:" + attr.getName());
                    return true;
                }
                return false;
            }
        });
        assertEquals(table.getAttributes().size(), 31);

        MetadataResolver resolver = new MetadataResolver(hdfsPath2, SchemaInterpretation.SalesforceLead, null,
                yarnConfiguration);
        resolver.calculateBasedOnExistingMetadata(table);
        assertEquals(table.getAttributes().size(), 31);

        if (!resolver.isMetadataFullyDefined()) {
            List<ColumnTypeMapping> unknown = resolver.getUnknownColumns();
            resolver = new MetadataResolver(hdfsPath2, SchemaInterpretation.SalesforceLead, unknown,
                    yarnConfiguration);
            resolver.calculateBasedOnExistingMetadata(table);
        }
        assertNotNull(table.getAttributeFromDisplayName("Some Column"));
        assertEquals(table.getAttributes().size(), 32);
    }

    @Test(groups = "functional")
    public void testDuplicateHeadersFail() {

    }

    @AfterClass(groups = "functional")
    public void cleanup() throws IOException {
        HdfsUtils.rmdir(yarnConfiguration, hdfsPath);
    }
}
