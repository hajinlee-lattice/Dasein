package com.latticeengines.pls.service.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.service.ModelMetadataService;
import com.latticeengines.pls.service.PlsFeatureFlagService;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.mchange.io.FileUtils;

public class ScoringFileMetadataServiceImplUnitTestNG {

    private static final Logger log = LoggerFactory.getLogger(ScoringFileMetadataServiceImplUnitTestNG.class);

    private ScoringFileMetadataServiceImpl scoringFileMetadataServiceImpl = new ScoringFileMetadataServiceImpl();
    private static final String standardAttrName1 = "Email";
    private static final String standardAttrName2 = "CompanyName";
    private static final String userDefinedModelAttributeName = "userDefinedModelAttributeName";
    // userDefinedScoreAttributeMappedSchemaColumnName is in standard schema
    private static final String userDefinedScoreAttributeCSVHeaderName = "Primary Country";
    private static final String userDefinedScoreAttributeMappedSchemaColumnName = "Country";
    // userDefinedScoreAttributeName is NOT in standard schema
    private static final String userDefinedScoreAttributeName = "someRandomName";

    @Test(groups = "unit")
    public void testMapRequiredField() throws IOException {
        URL csvFileUrl = ClassLoader
                .getSystemResource("com/latticeengines/pls/service/impl/scoringfilemetadataserviceimpl/table.json");
        Table t = JsonUtils.deserialize(new FileInputStream(new File(csvFileUrl.getPath())), Table.class);
        PythonScriptModelService pythonScriptModelService = new PythonScriptModelService();
        List<Attribute> attrs = pythonScriptModelService.getRequiredColumns(t);
        ScoringFileMetadataServiceImpl scoringFileMetadataService = new ScoringFileMetadataServiceImpl();

        FieldMappingDocument fieldMappingDocument = scoringFileMetadataService.getFieldMapping(
                Sets.newHashSet("Id", "Company Name", "City", "State"), attrs,
                SchemaRepository.instance().getMatchingAttributes(SchemaInterpretation.SalesforceLead));

        String expected = FileUtils.getContentsAsString(new File(ClassLoader
                .getSystemResource(
                        "com/latticeengines/pls/service/impl/scoringfilemetadataserviceimpl/expectedfieldmapping.json")
                .getPath()));
        List<?> expectedRaw = JsonUtils.deserialize(expected, List.class);
        List<FieldMapping> expectedMappings = JsonUtils.convertList(expectedRaw, FieldMapping.class);
        Assert.assertEquals(fieldMappingDocument.getFieldMappings().size(), expectedMappings.size());
        for (int i = 0; i < expectedMappings.size(); i++) {
            Assert.assertTrue(mappingEqual(expectedMappings.get(i), fieldMappingDocument.getFieldMappings().get(i)));
        }
    }

    private boolean mappingEqual(FieldMapping left, FieldMapping right) {
        boolean result = left.isMappedToLatticeField() == right.isMappedToLatticeField();
        if (!result) {
            return false;
        }
        if (left.getMappedField() == null && right.getMappedField() == null) {
            result = true;
        } else if (left.getMappedField() != null && right.getMappedField() != null) {
            result = left.getMappedField().equals(right.getMappedField());
        } else {
            return false;
        }
        if (!result) {
            return false;
        }
        if (left.getUserField() == null && right.getUserField() == null) {
            return true;
        } else if (left.getUserField() != null && right.getUserField() != null) {
            result = left.getUserField().equals(right.getUserField());
        } else {
            return false;
        }
        return result;
    }

    @Test(groups = "unit")
    public void testMapRequiredFieldPLS_7102() throws IOException {
        URL tableFileUrl = ClassLoader.getSystemResource(
                "com/latticeengines/pls/service/impl/scoringfilemetadataserviceimpl/tablePLS_7102.json");
        List<?> list = JsonUtils.deserialize(new FileInputStream(new File(tableFileUrl.getPath())), List.class);
        List<Attribute> attrs = JsonUtils.convertList(list, Attribute.class);

        ScoringFileMetadataServiceImpl scoringFileMetadataService = new ScoringFileMetadataServiceImpl();
        FieldMappingDocument fieldMappingDocument = scoringFileMetadataService.getFieldMapping(
                Sets.newHashSet("Company1", "Country", "Id", "Email", "City", "State"), attrs,
                SchemaRepository.instance().getMatchingAttributes(SchemaInterpretation.SalesforceLead));

        String expected = FileUtils.getContentsAsString(new File(ClassLoader
                .getSystemResource(
                        "com/latticeengines/pls/service/impl/scoringfilemetadataserviceimpl/expectedfieldmappingPLS_7102.json")
                .getPath()));

        List<?> expectedRaw = JsonUtils.deserialize(expected, List.class);
        List<FieldMapping> expectedMappings = JsonUtils.convertList(expectedRaw, FieldMapping.class);
        Assert.assertEquals(fieldMappingDocument.getFieldMappings().size(), expectedMappings.size());
        Collections.sort(expectedMappings, Comparator.comparing(FieldMapping::getUserField, 
                Comparator.nullsFirst(Comparator.naturalOrder())));
        Collections.sort(fieldMappingDocument.getFieldMappings(),
                Comparator.comparing(FieldMapping::getUserField, Comparator.nullsFirst(Comparator.naturalOrder())));
        for (int i = 0; i < expectedMappings.size(); i++) {
            Assert.assertTrue(mappingEqual(expectedMappings.get(i), fieldMappingDocument.getFieldMappings().get(i)));
        }

        ModelMetadataService modelMetadataService = Mockito.mock(ModelMetadataService.class);
        attrs = JsonUtils.convertList(list, Attribute.class);
        when(modelMetadataService.getRequiredColumns(anyString())).thenReturn(attrs);

        SourceFileService sourceFileService = Mockito.mock(SourceFileService.class);
        SourceFile sourceFile = new SourceFile();
        sourceFile.setName("mockfile");
        sourceFile.setPath(ClassLoader
                .getSystemResource(
                        "com/latticeengines/pls/service/impl/scoringfilemetadataserviceimpl/SOapuUI.Test3.csv")
                .getPath());
        when(sourceFileService.findByName(anyString())).thenReturn(sourceFile);
        Mockito.doNothing().when(sourceFileService).update(any(SourceFile.class));

        MetadataProxy metadataProxy = Mockito.mock(MetadataProxy.class);
        Mockito.doNothing().when(metadataProxy).createTable(anyString(), anyString(), any(Table.class));

        ModelSummary summary = new ModelSummary();
        summary.setId("modelid");
        summary.setSourceSchemaInterpretation(SchemaInterpretation.SalesforceLead.name());
        ModelSummaryProxy modelSummaryProxy = Mockito.mock(ModelSummaryProxy.class);
        when(modelSummaryProxy.findValidByModelId(anyString(), anyString())).thenReturn(summary);

        PlsFeatureFlagService plsFeatureFlagService = Mockito.mock(PlsFeatureFlagService.class);
        when(plsFeatureFlagService.isFuzzyMatchEnabled()).thenReturn(Boolean.FALSE);

        Configuration localFileSystemConfig = new Configuration();
        localFileSystemConfig.set(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS);

        ReflectionTestUtils.setField(scoringFileMetadataService, "modelMetadataService", modelMetadataService);
        ReflectionTestUtils.setField(scoringFileMetadataService, "sourceFileService", sourceFileService);
        ReflectionTestUtils.setField(scoringFileMetadataService, "metadataProxy", metadataProxy);
        ReflectionTestUtils.setField(scoringFileMetadataService, "modelSummaryProxy", modelSummaryProxy);
        ReflectionTestUtils.setField(scoringFileMetadataService, "yarnConfiguration", localFileSystemConfig);
        ReflectionTestUtils.setField(scoringFileMetadataService, "plsFeatureFlagService", plsFeatureFlagService);

        Tenant tenant = new Tenant();
        tenant.setPid(1L);
        tenant.setId("2");
        MultiTenantContext.setTenant(tenant);
        Table t = scoringFileMetadataService.saveFieldMappingDocument(sourceFile.getName(), summary.getId(),
                fieldMappingDocument);
        System.out.print(t);
        expected = FileUtils.getContentsAsString(new File(ClassLoader
                .getSystemResource(
                        "com/latticeengines/pls/service/impl/scoringfilemetadataserviceimpl/expectedsavedtablePLS_7102.json")
                .getPath()));
        Assert.assertEquals(t.toString().trim(), expected.trim());

    }

    @Test(groups = "unit")
    public void testResolveModelAttributeBasedOnFieldMapping() {
        List<Attribute> modelAttributes = generateTestModelAttributes();
        List<FieldMapping> fieldMappingList = generateTestFieldMappingDocument().getFieldMappings();
        scoringFileMetadataServiceImpl.resolveModelAttributeBasedOnFieldMapping(modelAttributes,
                generateTestFieldMappingDocument());
        Assert.assertEquals(modelAttributes.size(), fieldMappingList.size());
        assertUserDefinedAttributeInSchemaIsCorrectlySet(modelAttributes);
    }

    private void assertUserDefinedAttributeInSchemaIsCorrectlySet(List<Attribute> modelAttributes) {
        List<Attribute> countryAttr = modelAttributes.stream()
                .filter(attribute -> attribute.getDisplayName().equals(userDefinedScoreAttributeCSVHeaderName))
                .collect(Collectors.toList());
        Assert.assertTrue(countryAttr.size() == 1);
        Assert.assertEquals(countryAttr.get(0).getName(), userDefinedScoreAttributeMappedSchemaColumnName);
    }

    private List<Attribute> generateTestModelAttributes() {
        List<Attribute> modelAttributeList = new ArrayList<Attribute>();
        Attribute standardAttr1 = new Attribute();
        Attribute standardAttr2 = new Attribute();
        Attribute userDefinedModelAttribute = new Attribute();
        Attribute userDefinedScoreAttributeMappedSchemaAttribute = new Attribute();
        modelAttributeList.add(standardAttr1);
        modelAttributeList.add(standardAttr2);
        modelAttributeList.add(userDefinedModelAttribute);
        modelAttributeList.add(userDefinedScoreAttributeMappedSchemaAttribute);
        standardAttr1.setName(standardAttrName1);
        standardAttr1.setDisplayName(standardAttrName1);
        standardAttr2.setName(standardAttrName2);
        standardAttr2.setDisplayName(standardAttrName2);
        userDefinedModelAttribute.setName(userDefinedModelAttributeName);
        userDefinedModelAttribute.setDisplayName(userDefinedModelAttributeName);
        userDefinedScoreAttributeMappedSchemaAttribute.setName(userDefinedScoreAttributeMappedSchemaColumnName);
        userDefinedScoreAttributeMappedSchemaAttribute.setDisplayName(userDefinedScoreAttributeCSVHeaderName);

        return modelAttributeList;
    }

    private FieldMappingDocument generateTestFieldMappingDocument() {
        FieldMappingDocument fieldMappingDocument = new FieldMappingDocument();
        List<FieldMapping> fieldMappingList = new ArrayList<FieldMapping>();
        fieldMappingDocument.setFieldMappings(fieldMappingList);

        FieldMapping unMappedScoreHeaderField = new FieldMapping();
        unMappedScoreHeaderField.setFieldType(UserDefinedType.TEXT);
        unMappedScoreHeaderField.setMappedToLatticeField(false);
        unMappedScoreHeaderField.setUserField(userDefinedScoreAttributeName);

        FieldMapping mappedToUserProvidedRequiredColumnField = new FieldMapping();
        mappedToUserProvidedRequiredColumnField.setMappedToLatticeField(true);
        mappedToUserProvidedRequiredColumnField.setMappedField(userDefinedModelAttributeName);
        mappedToUserProvidedRequiredColumnField.setUserField(userDefinedModelAttributeName);

        FieldMapping mappedToModelRequiredColumnField1 = new FieldMapping();
        mappedToModelRequiredColumnField1.setUserField(standardAttrName1);
        mappedToModelRequiredColumnField1.setMappedField(standardAttrName1);
        mappedToModelRequiredColumnField1.setMappedToLatticeField(true);
        FieldMapping mappedToModelRequiredColumnField2 = new FieldMapping();
        mappedToModelRequiredColumnField2.setUserField(standardAttrName2);
        mappedToModelRequiredColumnField2.setMappedField(standardAttrName2);
        mappedToModelRequiredColumnField2.setMappedToLatticeField(true);

        FieldMapping mappedToSchemaColumnField = new FieldMapping();
        mappedToSchemaColumnField.setMappedToLatticeField(true);
        mappedToSchemaColumnField.setUserField(userDefinedScoreAttributeCSVHeaderName);
        mappedToSchemaColumnField.setMappedField(userDefinedScoreAttributeMappedSchemaColumnName);

        fieldMappingList.add(unMappedScoreHeaderField);
        fieldMappingList.add(mappedToUserProvidedRequiredColumnField);
        fieldMappingList.add(mappedToModelRequiredColumnField1);
        fieldMappingList.add(mappedToModelRequiredColumnField2);
        fieldMappingList.add(mappedToSchemaColumnField);

        fieldMappingDocument.setIgnoredFields(
                Arrays.asList(InterfaceName.Website.name(), InterfaceName.DUNS.name(), InterfaceName.City.name(),
                        InterfaceName.State.name(), InterfaceName.PhoneNumber.name(), InterfaceName.PostalCode.name()));

        return fieldMappingDocument;
    }
}
