package com.latticeengines.pls.metadata.resolution;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBaseDeprecated;
import com.latticeengines.transform.v2_0_25.common.JsonUtils;

public class MetadataResolverTestNG extends PlsFunctionalTestNGBaseDeprecated {

    private static Logger log = LoggerFactory.getLogger(MetadataResolverTestNG.class);

    @Autowired
    private Configuration yarnConfiguration;

    String hdfsPath = "/tmp/test_metadata_resolution";

    String hdfsPath2 = "/tmp/test_metadata_resolution2";

    @BeforeClass(groups = "functional")
    public void setup() throws IOException {
        String path = ClassLoader
                .getSystemResource("com/latticeengines/pls/service/impl/fileuploadserviceimpl/file1.csv").getPath();

        HdfsUtils.rmdir(yarnConfiguration, hdfsPath);
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, path, hdfsPath);

        path = ClassLoader.getSystemResource("com/latticeengines/pls/metadata/csvfiles/sample_lead.csv").getPath();

        HdfsUtils.rmdir(yarnConfiguration, hdfsPath2);
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, path, hdfsPath2);
    }

    @Test(groups = "functional")
    public void getFieldMappingsTest() {
        MetadataResolver resolver = new MetadataResolver(hdfsPath, yarnConfiguration, null);

        Table table = SchemaRepository.instance().getSchema(SchemaInterpretation.SalesforceAccount);
        FieldMappingDocument fieldMappingDocument = resolver.getFieldMappingsDocumentBestEffort(table);

        Set<String> expectedUnknownColumns = Sets.newHashSet(
                new String[] { "Some Column", "Boolean Column", "Number Column", "Almost Boolean Column", "Date" });
        expectedUnknownColumns
                .addAll(SchemaRepository.instance().getMatchingAttributes(SchemaInterpretation.SalesforceAccount).stream()
                        .flatMap(attr -> attr.getAllowedDisplayNames().stream()).collect(Collectors.toSet()));
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getMappedField() == null) {
                fieldMapping.setMappedField(fieldMapping.getUserField());

                assertTrue(expectedUnknownColumns.stream().anyMatch(fieldMapping.getUserField()::equalsIgnoreCase));
            }
        }
        resolver.setFieldMappingDocument(fieldMappingDocument);

        table = SchemaRepository.instance().getSchema(SchemaInterpretation.SalesforceAccount);
        resolver.calculateBasedOnFieldMappingDocument(table);

        assertTrue(resolver.isMetadataFullyDefined());
        table = resolver.getMetadata();

        assertEquals(table.getAttribute(InterfaceName.Id).getDisplayName(), "Account iD");
        assertEquals(table.getAttribute(InterfaceName.Website).getDisplayName(), "Website");
        assertEquals(table.getAttribute(InterfaceName.Event).getDisplayName(), "Event");
        assertEquals(table.getAttribute(InterfaceName.Country).getDisplayName(), "Billing Country");
        assertEquals(table.getAttribute(InterfaceName.CompanyName).getDisplayName(), "ACCOUNT Name");
        assertEquals(table.getAttribute(InterfaceName.LastModifiedDate).getDisplayName(), "Last Modified Date");
        assertEquals(table.getAttribute(InterfaceName.IsClosed).getDisplayName(), "Is Closed");
        assertEquals(table.getAttribute(InterfaceName.StageName).getDisplayName(), "Stage");
        assertNull(table.getAttribute(InterfaceName.AnnualRevenue));

        Attribute attribute = table.getAttribute("Some_Column");
        assertEquals(attribute.getTags().size(), 1);
        assertEquals(attribute.getTags().get(0), ModelingMetadata.INTERNAL_TAG);
        assertEquals(attribute.getCategory(), ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION);
        assertEquals(attribute.getPhysicalDataType(), UserDefinedType.TEXT.getAvroType().toString().toLowerCase());
        assertEquals(attribute.getFundamentalType(), ModelingMetadata.FT_ALPHA);
        assertEquals(attribute.getStatisticalType(), ModelingMetadata.NOMINAL_STAT_TYPE);

        attribute = table.getAttribute("Boolean_Column");
        assertEquals(attribute.getPhysicalDataType(), UserDefinedType.BOOLEAN.getAvroType().toString().toLowerCase());
        assertEquals(attribute.getFundamentalType(), ModelingMetadata.FT_BOOLEAN);
        assertEquals(attribute.getStatisticalType(), ModelingMetadata.NOMINAL_STAT_TYPE);
        attribute = table.getAttribute("Number_Column");
        assertEquals(attribute.getPhysicalDataType(), UserDefinedType.NUMBER.getAvroType().toString().toLowerCase());
        assertEquals(attribute.getFundamentalType(), ModelingMetadata.FT_NUMERIC);
        assertEquals(attribute.getStatisticalType(), ModelingMetadata.RATIO_STAT_TYPE);
        attribute = table.getAttribute("Almost_Boolean_Column");
        assertEquals(attribute.getPhysicalDataType(), UserDefinedType.TEXT.getAvroType().toString().toLowerCase());
        assertEquals(attribute.getFundamentalType(), ModelingMetadata.FT_ALPHA);
        assertEquals(attribute.getStatisticalType(), ModelingMetadata.NOMINAL_STAT_TYPE);
        attribute = table.getAttribute("Date");
        assertEquals(attribute.getPhysicalDataType(), UserDefinedType.DATE.getAvroType().toString().toLowerCase());
        assertEquals(attribute.getLogicalDataType(), LogicalDataType.Date);
        assertEquals(attribute.getFundamentalType(), FundamentalType.DATE.getName());
        attribute = table.getAttribute("IsClosed");
        assertEquals(attribute.getApprovedUsage(), Arrays.asList(ModelingMetadata.NONE_APPROVED_USAGE));
        assertEquals(attribute.getFundamentalType(), ModelingMetadata.FT_BOOLEAN);
        attribute = table.getAttribute("StageName");
        assertEquals(attribute.getApprovedUsage(), Arrays.asList(ModelingMetadata.NONE_APPROVED_USAGE));
        assertEquals(attribute.getFundamentalType(), ModelingMetadata.FT_ALPHA);

        for (Attribute a : table.getAttributes()) {
            assertNotEquals(a.getTags(), 0);
            assertEquals(a.getTags().get(0), ModelingMetadata.INTERNAL_TAG);
            assertEquals(attribute.getCategory(), ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION);
        }
        assertTrue(resolver.isMetadataFullyDefined());
    }

    @Test(groups = "functional")
    public void getMappingFromDocument_mapUnknownColumnToLatticeAttr_assertMappedCorrectly() {
        MetadataResolver resolver = new MetadataResolver(hdfsPath, yarnConfiguration, null);
        FieldMappingDocument fieldMappingDocument = resolver.getFieldMappingsDocumentBestEffort(
                SchemaRepository.instance().getSchema(SchemaInterpretation.SalesforceAccount));
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
        resolver.calculateBasedOnFieldMappingDocument(
                SchemaRepository.instance().getSchema(SchemaInterpretation.SalesforceAccount));

        Table table = resolver.getMetadata();
        assertEquals(table.getAttribute(InterfaceName.Industry).getDisplayName(), "Some Column");
    }

    @Test(groups = "functional")
    public void getMappingFromDocument_mapUnknownColumnToIgnore_assertColumnsIgnored() {
        MetadataResolver resolver = new MetadataResolver(hdfsPath, yarnConfiguration, null);
        Table table = SchemaRepository.instance().getSchema(SchemaInterpretation.SalesforceAccount);
        FieldMappingDocument fieldMappingDocument = resolver.getFieldMappingsDocumentBestEffort(table);

        assertFalse(resolver.isMetadataFullyDefined());
        List<String> ignoredFields = new ArrayList<>();
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getMappedField() == null) {
                ignoredFields.add(fieldMapping.getUserField());
            }
        }
        fieldMappingDocument.setIgnoredFields(ignoredFields);
        resolver.setFieldMappingDocument(fieldMappingDocument);
        resolver.calculateBasedOnFieldMappingDocument(
                SchemaRepository.instance().getSchema(SchemaInterpretation.SalesforceAccount));

        assertTrue(resolver.isMetadataFullyDefined());
        table = resolver.getMetadata();
        for (Attribute attribute : table.getAttributes()) {
            assertFalse(ignoredFields.contains(attribute.getDisplayName()));
        }
    }

    @Test(groups = "functional")
    public void getMappingFromDocument_setColumnToSpecificType_assertTypeIsSet() {
        MetadataResolver resolver = new MetadataResolver(hdfsPath, yarnConfiguration, null);
        FieldMappingDocument fieldMappingDocument = resolver.getFieldMappingsDocumentBestEffort(
                SchemaRepository.instance().getSchema(SchemaInterpretation.SalesforceAccount));
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
        resolver.calculateBasedOnFieldMappingDocument(
                SchemaRepository.instance().getSchema(SchemaInterpretation.SalesforceAccount));
        assertTrue(resolver.isMetadataFullyDefined());

        Table table = resolver.getMetadata();
        Attribute attribute = table.getAttribute("Almost_Boolean_Column");
        assertEquals(attribute.getPhysicalDataType(), UserDefinedType.BOOLEAN.getAvroType().toString().toLowerCase());
    }

    @Test(groups = "functional")
    public void testCalculateBasedOnExistingMetadata() throws IOException {
        String path = ClassLoader
                .getSystemResource("com/latticeengines/pls/service/impl/fileuploadserviceimpl/table.json").getPath();

        Table table = JsonUtils.deserialize(FileUtils.readFileToString(new File(path), Charset.defaultCharset()),
                Table.class);
        table.getAttributeFromDisplayName("Some Column").setApprovedUsage(ApprovedUsage.NONE.toString());

        final Table schema = SchemaRepository.instance().getSchema(SchemaInterpretation.SalesforceLead);
        Iterables.removeIf(table.getAttributes(), new Predicate<Attribute>() {
            @Override
            public boolean apply(@Nullable Attribute attr) {
                List<String> approvedUsages = attr.getApprovedUsage();
                List<String> tags = attr.getTags();

                if (schema.getAttribute(attr.getName()) == null
                        && (approvedUsages == null || approvedUsages.isEmpty()
                                || approvedUsages.get(0).equals(ApprovedUsage.NONE.toString())) //
                        || (tags == null || tags.isEmpty() || !tags.get(0).equals(Tag.INTERNAL.toString()))) {
                    log.info("Removing attr:" + attr.getName());
                    return true;
                }
                return false;
            }
        });

        assertEquals(table.getAttributes().size(), 30);

        MetadataResolver resolver = new MetadataResolver(hdfsPath2, yarnConfiguration, null);
        List<FieldMapping> fieldMappings = resolver.calculateBasedOnExistingMetadata(table);
        FieldMappingDocument fieldMappingDocument = new FieldMappingDocument();
        fieldMappingDocument.setFieldMappings(fieldMappings);
        resolver.setFieldMappingDocument(fieldMappingDocument);
        resolver.calculateBasedOnFieldMappingDocumentAndTable(table);

        boolean foundSomeColumn = false;
        for (FieldMapping fieldMapping : fieldMappings) {
            log.info(String.format("The field mapping is: %s", fieldMapping.getUserField()));
            if (fieldMapping.getUserField().equals("Some Column")) {
                foundSomeColumn = true;
            }
        }
        assertTrue(foundSomeColumn);
        assertEquals(fieldMappings.size(), 34);
        assertEquals(table.getAttributes().size(), 34);
        assertNotNull(table.getAttribute("BusinessCountry"));
        assertNotNull(table.getAttribute("avro_1to300"));
        assertEquals(table.getAttributes().get(0).getDisplayName(), "LEAD");
        assertEquals(table.getAttributes().get(1).getDisplayName(), "1to300");
        assertEquals(table.getAttributes().get(33).getDisplayName(), "SourceColumn");
    }

    @AfterClass(groups = "functional")
    public void cleanup() throws IOException {
        HdfsUtils.rmdir(yarnConfiguration, hdfsPath);
    }
}
