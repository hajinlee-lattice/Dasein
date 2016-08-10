package com.latticeengines.pls.metadata.resolution;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertFalse;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import com.latticeengines.domain.exposed.metadata.*;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;

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

        path = ClassLoader
                .getSystemResource(
                        "com/latticeengines/pls/end2end/selfServiceModeling/csvfiles/Hootsuite_PLS132_LP3_ScoringLead_20160330_165806_modified.csv")
                .getPath();

        HdfsUtils.rmdir(yarnConfiguration, hdfsPath2);
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, path, hdfsPath2);
    }

    @Test(groups = "functional")
    public void getFieldMappingsTest() {
        MetadataResolver resolver = new MetadataResolver(hdfsPath, SchemaInterpretation.SalesforceAccount,
                yarnConfiguration, null);
        FieldMappingDocument fieldMappingDocument = resolver.getFieldMappingsDocumentBestEffort();

        Set<String> expectedUnknownColumns = Sets.newHashSet(new String[] { "Some Column", "Boolean Column",
                "Number Column", "Almost Boolean Column" });

        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getMappedField() == null) {
                fieldMapping.setMappedField(fieldMapping.getUserField());

                assertTrue(expectedUnknownColumns.contains(fieldMapping.getUserField()));
            }
        }
        resolver.setFieldMappingDocument(fieldMappingDocument);
        resolver.calculateBasedOnFieldMappingDocument();

        assertTrue(resolver.isMetadataFullyDefined());
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
        assertEquals(attribute.getPhysicalDataType(), UserDefinedType.TEXT.getAvroType().toString().toLowerCase());
        assertEquals(attribute.getFundamentalType(), ModelingMetadata.FT_ALPHA);
        assertEquals(attribute.getStatisticalType(), ModelingMetadata.NOMINAL_STAT_TYPE);

        attribute = table.getAttribute("Boolean_Column");
        assertEquals(attribute.getPhysicalDataType(), UserDefinedType.BOOLEAN.getAvroType().toString().toLowerCase());
        attribute = table.getAttribute("Number_Column");
        assertEquals(attribute.getPhysicalDataType(), UserDefinedType.NUMBER.getAvroType().toString().toLowerCase());
        attribute = table.getAttribute("Almost_Boolean_Column");
        assertEquals(attribute.getPhysicalDataType(), UserDefinedType.TEXT.getAvroType().toString().toLowerCase());

        for (Attribute a : table.getAttributes()) {
            assertNotEquals(a.getTags(), 0);
            assertEquals(a.getTags().get(0), ModelingMetadata.INTERNAL_TAG);
            assertEquals(attribute.getCategory(), ModelingMetadata.CATEGORY_LEAD_INFORMATION);
        }
        assertTrue(resolver.isMetadataFullyDefined());
    }

    @Test(groups = "functional")
    public void getMappingFromDocument_mapUnknownColumnToLatticeAttr_assertMappedCorrectly() {
        MetadataResolver resolver = new MetadataResolver(hdfsPath, SchemaInterpretation.SalesforceAccount,
                yarnConfiguration, null);
        FieldMappingDocument fieldMappingDocument = resolver.getFieldMappingsDocumentBestEffort();
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getMappedField() == null) {
                if (fieldMapping.getUserField().equals("Some Column")) {
                    fieldMapping.setMappedToLatticeField(true);
                    fieldMapping.setMappedField("Industry");
                } else {
                    fieldMapping.setMappedField(fieldMapping.getUserField());
                }
            }
        }

        resolver.setFieldMappingDocument(fieldMappingDocument);
        resolver.calculateBasedOnFieldMappingDocument();

        Table table = resolver.getMetadata();
        assertEquals(table.getAttribute(InterfaceName.Industry).getDisplayName(), "Some Column");
    }

    @Test(groups = "functional")
    public void getMappingFromDocument_mapUnknownColumnToIgnore_assertColumnsIgnored() {
        MetadataResolver resolver = new MetadataResolver(hdfsPath, SchemaInterpretation.SalesforceAccount,
                yarnConfiguration, null);
        FieldMappingDocument fieldMappingDocument = resolver.getFieldMappingsDocumentBestEffort();

        assertFalse(resolver.isMetadataFullyDefined());
        List<String> ignoredFields = new ArrayList<>();
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getMappedField() == null) {
                ignoredFields.add(fieldMapping.getUserField());
            }
        }
        fieldMappingDocument.setIgnoredFields(ignoredFields);
        resolver.setFieldMappingDocument(fieldMappingDocument);
        resolver.calculateBasedOnFieldMappingDocument();

        assertTrue(resolver.isMetadataFullyDefined());
        Table table = resolver.getMetadata();
        for (Attribute attribute : table.getAttributes()) {
            if (ignoredFields.contains(attribute.getDisplayName())) {
                log.info("The ignored field is " + attribute.getDisplayName());
                assertEquals(attribute.getApprovedUsage(), Arrays.asList(ModelingMetadata.NONE_APPROVED_USAGE));
            }
        }
    }

    @Test(groups = "functional")
    public void getMappingFromDocument_setColumnToSpecificType_assertTypeIsSet() {
        MetadataResolver resolver = new MetadataResolver(hdfsPath, SchemaInterpretation.SalesforceAccount,
                yarnConfiguration, null);
        FieldMappingDocument fieldMappingDocument = resolver.getFieldMappingsDocumentBestEffort();
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getMappedField() == null) {
                if (fieldMapping.getUserField().equals("Almost Boolean Column")) {
                    fieldMapping.setMappedField(fieldMapping.getUserField());
                    fieldMapping.setFieldType(UserDefinedType.BOOLEAN);
                } else {
                    fieldMapping.setMappedField(fieldMapping.getUserField());
                }
            }
        }

        resolver.setFieldMappingDocument(fieldMappingDocument);
        resolver.calculateBasedOnFieldMappingDocument();
        assertTrue(resolver.isMetadataFullyDefined());

        Table table = resolver.getMetadata();
        Attribute attribute = table.getAttribute("Almost_Boolean_Column");
        assertEquals(attribute.getPhysicalDataType(), UserDefinedType.BOOLEAN.getAvroType().toString().toLowerCase());
    }

    @Test(groups = "functional")
    public void testCalculateBasedOnExistingMetadata() throws IOException {
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
        assertEquals(table.getAttributes().size(), 30);

        MetadataResolver resolver = new MetadataResolver(hdfsPath2, SchemaInterpretation.SalesforceLead,
                yarnConfiguration, null);
        List<FieldMapping> fieldMappings = resolver.calculateBasedOnExistingMetadata(table);
        FieldMappingDocument fieldMappingDocument = new FieldMappingDocument();
        fieldMappingDocument.setFieldMappings(fieldMappings);
        resolver = new MetadataResolver(hdfsPath2, table, yarnConfiguration, fieldMappingDocument);
        resolver.calculateBasedOnFieldMappingDocumentAndTable();

        boolean foundSomeColumn = false;
        for (FieldMapping fieldMapping : fieldMappings) {
            log.info(String.format("The field mapping is: %s", fieldMapping.getUserField()));
            if (fieldMapping.getUserField().equals("Some Column")) {
                foundSomeColumn = true;
            }
        }
        assertTrue(foundSomeColumn);
        assertEquals(fieldMappings.size(), 33);
        assertEquals(table.getAttributes().size(), 33);
        assertNotNull(table.getAttribute("BusinessCountry"));
        assertEquals(table.getAttributes().get(0).getDisplayName(), "LEAD");
        assertEquals(table.getAttributes().get(32).getDisplayName(), "SourceColumn");
    }

    @AfterClass(groups = "functional")
    public void cleanup() throws IOException {
        HdfsUtils.rmdir(yarnConfiguration, hdfsPath);
    }
}
