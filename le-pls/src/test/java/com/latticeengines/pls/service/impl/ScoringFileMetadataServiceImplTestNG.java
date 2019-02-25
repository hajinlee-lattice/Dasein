package com.latticeengines.pls.service.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.app.exposed.service.AttributeService;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.closeable.resource.CloseableResourcePool;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryProvenanceProperty;
import com.latticeengines.domain.exposed.pls.ProvenancePropertyName;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBaseDeprecated;
import com.latticeengines.pls.service.ModelMetadataService;
import com.latticeengines.pls.service.PlsFeatureFlagService;
import com.latticeengines.pls.service.ScoringFileMetadataService;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

public class ScoringFileMetadataServiceImplTestNG extends PlsFunctionalTestNGBaseDeprecated {

    private static final String PATH = "com/latticeengines/pls/service/impl/fileuploadserviceimpl/file2.csv";
    private File csvFile;
    private CloseableResourcePool closeableResourcePool;
    private String displayName;
    private ModelSummary MODEL_SUMMARY = new ModelSummary();
    private String MODEL_SUMMARY_ID = "MODEL_SUMMARY_ID";
    private String SCORE_FILE_METADATA_TEST = "SCORE_FILE_METADATA_TEST/";
    private SourceFile SOURCE_FILE = new SourceFile();

    @Autowired
    private ScoringFileMetadataService scoringFileMetadataService;

    @Autowired
    private Configuration yarnConfiguration;

    @BeforeClass(groups = { "functional" })
    public void setup() throws Exception {
        URL csvFileUrl = ClassLoader.getSystemResource(PATH);
        csvFile = new File(csvFileUrl.getFile());
        closeableResourcePool = new CloseableResourcePool();
        displayName = "file2.csv";
        HdfsUtils.rmdir(yarnConfiguration, SCORE_FILE_METADATA_TEST);
        HdfsUtils.copyFromLocalDirToHdfs(yarnConfiguration, csvFileUrl.getFile(),
                SCORE_FILE_METADATA_TEST + "file2.csv");
    }

    @AfterClass(groups = { "functional" })
    public void teardown() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, SCORE_FILE_METADATA_TEST);
    }

    @Test(groups = "functional", enabled = false)
    public void testValidateHeaderFieldsWithEmptyHeaders() throws FileNotFoundException {
        InputStream stream = new FileInputStream(csvFile);
        boolean exceptionThrown = false;
        try {
            scoringFileMetadataService.validateHeaderFields(stream, closeableResourcePool, displayName);
            assertTrue(exceptionThrown, "should have thrown exception");
        } catch (Exception e) {
            exceptionThrown = true;
            assertEquals(((LedpException) e).getCode(), LedpCode.LEDP_18096);
        }
        assertTrue(exceptionThrown);
    }

    @Test(groups = "functional")
    public void testFieldMapping_whenModelFieldsNotMapped_mappedToSchemaRepository() {
        List<Attribute> attrs = new ArrayList<>();
        Attribute ACCOUNT_ID = new Attribute();
        ACCOUNT_ID.setName("ACCOUNT_ID");
        ACCOUNT_ID.setDisplayName("ACCOUNT_ID");
        attrs.add(ACCOUNT_ID);

        Attribute country = new Attribute();
        country.setName("country");
        country.setDisplayName("country");
        attrs.add(country);

        Attribute accountName = new Attribute();
        accountName.setName("account name");
        accountName.setDisplayName("Account name");
        attrs.add(accountName);

        Attribute event = new Attribute();
        event.setName("event");
        event.setDisplayName("event");
        attrs.add(event);

        MODEL_SUMMARY.setId(MODEL_SUMMARY_ID);
        MODEL_SUMMARY.setSourceSchemaInterpretation(SchemaInterpretation.SalesforceAccount.toString());
        ModelSummaryProvenanceProperty provenanceProperty = new ModelSummaryProvenanceProperty();
        provenanceProperty.setModelSummary(MODEL_SUMMARY);
        provenanceProperty.setOption(ProvenancePropertyName.ExcludePropdataColumns.toString());
        provenanceProperty.setValue("false");
        MODEL_SUMMARY.addModelSummaryProvenanceProperty(provenanceProperty);

        SOURCE_FILE.setPath(SCORE_FILE_METADATA_TEST + "file2.csv");
        SOURCE_FILE.setName("file2.csv");

        ModelSummaryProxy modelSummaryProxy = Mockito.mock(ModelSummaryProxy.class);
        when(modelSummaryProxy.findValidByModelId(anyString(), eq(MODEL_SUMMARY_ID))).thenReturn(MODEL_SUMMARY);

        PlsFeatureFlagService plsFeatureFlagService = Mockito.mock(PlsFeatureFlagService.class);
        when(plsFeatureFlagService.isFuzzyMatchEnabled()).thenReturn(false);

        ModelMetadataService modelMetadataService = Mockito.mock(ModelMetadataService.class);
        when(modelMetadataService.getRequiredColumns(anyString())).thenReturn(attrs);

        SourceFileService sourceFileService = Mockito.mock(SourceFileService.class);
        when(sourceFileService.findByName("file2.csv")).thenReturn(SOURCE_FILE);

        Mockito.doNothing().when(sourceFileService).update(any(SourceFile.class));

        ReflectionTestUtils.setField(scoringFileMetadataService, "modelMetadataService", modelMetadataService);
        ReflectionTestUtils.setField(scoringFileMetadataService, "sourceFileService", sourceFileService);
        ReflectionTestUtils.setField(scoringFileMetadataService, "plsFeatureFlagService", plsFeatureFlagService);
        ReflectionTestUtils.setField(scoringFileMetadataService, "modelSummaryProxy", modelSummaryProxy);

        Tenant t = new Tenant();
        t.setId("t1");
        t.setPid(-1L);
        MultiTenantContext.setTenant(t);
        FieldMappingDocument fieldMappingDocument = scoringFileMetadataService
                .mapRequiredFieldsWithFileHeaders("file2.csv", MODEL_SUMMARY_ID);
        List<FieldMapping> fieldMappings = fieldMappingDocument.getFieldMappings();

        Set<String> headerFields = new HashSet<>(Arrays.asList("Account iD", "website", "Event", "Billing Country",
                "ACCOUNT Name", "Some Column", "Last Modified Date", "Boolean Column", "Number Column"));
        for (FieldMapping fieldMapping : fieldMappings) {
            headerFields.remove(fieldMapping.getUserField());
            if (fieldMapping.getUserField() != null) {
                switch (fieldMapping.getUserField()) {
                case "website":
                    assertTrue(fieldMapping.isMappedToLatticeField());
                    break;
                default:
                    break;
                }
            }
        }
        System.out.println(fieldMappings);
        assertTrue(headerFields.isEmpty());
    }

    @Test(groups = "functional", enabled = true)
    public void testSaveFieldMappingDocument() {
        List<Attribute> attrs = new ArrayList<>();
        Attribute a = new Attribute();
        a.setName("A");
        a.setDisplayName("AD");
        attrs.add(a);

        Attribute b = new Attribute();
        b.setName("B");
        b.setDisplayName("BD");
        attrs.add(b);

        Attribute c = new Attribute();
        c.setName("C");
        c.setDisplayName("CD");
        attrs.add(c);

        ModelMetadataService modelMetadataService = Mockito.mock(ModelMetadataService.class);
        when(modelMetadataService.getRequiredColumns(anyString())).thenReturn(attrs);

        Set<String> attributeNames = new HashSet<>();
        attributeNames.add("C");
        attributeNames.add("C_1");
        when(modelMetadataService.getLatticeAttributeNames(anyString())).thenReturn(attributeNames);

        SourceFileService sourceFileService = Mockito.mock(SourceFileService.class);
        SourceFile sourceFile = new SourceFile();
        sourceFile.setName("mockfile");
        sourceFile.setPath(ClassLoader
                .getSystemResource("com/latticeengines/pls/service/impl/scoringfilemetadataserviceimpl/mockfile.csv")
                .getPath());
        when(sourceFileService.findByName(anyString())).thenReturn(sourceFile);
        Mockito.doNothing().when(sourceFileService).update(any(SourceFile.class));

        MetadataProxy metadataProxy = Mockito.mock(MetadataProxy.class);
        Mockito.doNothing().when(metadataProxy).createTable(anyString(), anyString(), any(Table.class));

        ModelSummary summary = new ModelSummary();
        summary.setId("modelid");
        summary.setSourceSchemaInterpretation(SchemaInterpretation.Account.name());
        ModelSummaryProxy modelSummaryProxy = Mockito.mock(ModelSummaryProxy.class);
        when(modelSummaryProxy.findValidByModelId(anyString(), anyString())).thenReturn(summary);

        Configuration localFileSystemConfig = new Configuration();
        localFileSystemConfig.set(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS);

        ReflectionTestUtils.setField(scoringFileMetadataService, "modelMetadataService", modelMetadataService);
        ReflectionTestUtils.setField(scoringFileMetadataService, "sourceFileService", sourceFileService);
        ReflectionTestUtils.setField(scoringFileMetadataService, "metadataProxy", metadataProxy);
        ReflectionTestUtils.setField(scoringFileMetadataService, "modelSummaryProxy", modelSummaryProxy);
        ReflectionTestUtils.setField(scoringFileMetadataService, "yarnConfiguration", localFileSystemConfig);

        FieldMappingDocument fieldMappingDocument = new FieldMappingDocument();

        FieldMapping mapping = new FieldMapping();
        mapping.setUserField("SomeId");
        mapping.setMappedField("A");
        mapping.setMappedToLatticeField(true);
        mapping.setFieldType(UserDefinedType.NUMBER);

        FieldMapping mapping2 = new FieldMapping();
        mapping2.setUserField("SomeStr");
        mapping2.setMappedField("B");
        mapping2.setMappedToLatticeField(true);
        mapping2.setFieldType(UserDefinedType.TEXT);

        FieldMapping mapping3 = new FieldMapping();
        mapping3.setUserField("C_1");
        mapping3.setMappedToLatticeField(false);
        mapping3.setFieldType(UserDefinedType.TEXT);

        FieldMapping mapping4 = new FieldMapping();
        mapping4.setUserField("C");
        mapping4.setMappedToLatticeField(false);
        mapping4.setFieldType(UserDefinedType.TEXT);

        fieldMappingDocument.setRequiredFields(Collections.singletonList("A"));
        fieldMappingDocument.setFieldMappings(
                new ArrayList<>(Arrays.asList(new FieldMapping[] { mapping, mapping2, mapping3, mapping4 })));
        fieldMappingDocument.setIgnoredFields(Collections.emptyList());

        Tenant t = new Tenant();
        t.setId("t1");
        t.setPid(-1L);
        MultiTenantContext.setTenant(t);
        Table table = scoringFileMetadataService.saveFieldMappingDocument("csvfilename", summary.getId(),
                fieldMappingDocument);

        assertEquals(table.getAttributes().size(),
                5 + SchemaRepository.instance().getMatchingAttributes(SchemaInterpretation.Account).size());
        assertNotNull(table.getAttribute("C_1"));
        assertNotNull(table.getAttribute("C_2"));
    }

    @Test(groups = "functional")
    public void testValidateHeadersWithDataCloudAttr() {
        Set<String> headers = new HashSet<String>();
        headers.add("A");
        headers.add("B");
        headers.add("C");

        Tenant t = new Tenant();
        t.setId("tena");
        t.setPid(-1L);
        MultiTenantContext.setTenant(t);
        LeadEnrichmentAttribute attr = new LeadEnrichmentAttribute();
        attr.setDisplayName("A");
        attr.setFieldName("A");

        BatonService batonService = Mockito.mock(BatonService.class);
        AttributeService attributeService = Mockito.mock(AttributeService.class);
        when(batonService.isEnabled(any(CustomerSpace.class), any(LatticeFeatureFlag.class))).thenReturn(false);
        when(attributeService.getAttributes(any(Tenant.class), anyString(), any(Category.class), anyString(),
                anyBoolean(), anyInt(), anyInt(), anyBoolean())).thenReturn(Collections.singletonList(attr));
        ReflectionTestUtils.setField(scoringFileMetadataService, "batonService", batonService);
        ReflectionTestUtils.setField(scoringFileMetadataService, "attributeService", attributeService);
        try {
            scoringFileMetadataService.validateHeadersWithDataCloudAttr(headers);
        } catch (Exception e) {
            assertEquals(((LedpException) e).getCode(), LedpCode.LEDP_18109);
        }
    }
}
