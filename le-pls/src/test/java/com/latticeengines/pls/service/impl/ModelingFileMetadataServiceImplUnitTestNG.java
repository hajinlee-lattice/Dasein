package com.latticeengines.pls.service.impl;

import static org.mockito.ArgumentMatchers.anyString;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.metadata.standardschemas.ImportWorkflowSpec;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinition;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;
import com.latticeengines.domain.exposed.pls.frontend.LatticeSchemaField;
import com.latticeengines.domain.exposed.pls.frontend.RequiredType;
import com.latticeengines.domain.exposed.pls.frontend.ValidateFieldDefinitionsRequest;
import com.latticeengines.domain.exposed.pls.frontend.ValidateFieldDefinitionsResponse;
import com.latticeengines.pls.service.ImportWorkflowSpecService;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.pls.util.ImportWorkflowUtilsTestNG;

public class ModelingFileMetadataServiceImplUnitTestNG {

    @InjectMocks
    private ModelingFileMetadataServiceImpl modelingFileMetadataService;

    @Mock
    private SourceFileService sourceFileService;

    @Mock
    private ImportWorkflowSpecService importWorkflowSpecService;

    private ImportWorkflowSpec importWorkflowSpec;
    @BeforeClass(groups = "unit")
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        SourceFile sourceFile = new SourceFile();
        Mockito.doReturn(sourceFile).when(sourceFileService).findByName(anyString());
        // Generate Spec Java class from resource file.
        importWorkflowSpec = ImportWorkflowUtilsTestNG.pojoFromJsonResourceFile(
                "com/latticeengines/pls/util/test-contact-spec.json",
                ImportWorkflowSpec.class);
        Mockito.doReturn(importWorkflowSpec).when(importWorkflowSpecService).loadSpecFromS3(anyString(), anyString());
    }

    @Test(groups = "unit")
    public void checkGetLatticeSchemaFields() {
        List<LatticeSchemaField> latticeSchemaFields = modelingFileMetadataService
                .getSchemaToLatticeSchemaFields(SchemaInterpretation.Account);
        Assert.assertNotNull(latticeSchemaFields);
        Assert.assertTrue(latticeSchemaFields.size() > 0);
        LatticeSchemaField idField = null;
        for (LatticeSchemaField latticeSchemaField : latticeSchemaFields) {
            if (latticeSchemaField.getName().equals(InterfaceName.AccountId.name())) {
                idField = latticeSchemaField;
                break;
            }
        }
        Assert.assertNotNull(idField);
        Assert.assertEquals(idField.getRequiredType(), RequiredType.Required);
    }

    @Test(groups = "unit")
    public void testValidation() throws Exception {
        ValidateFieldDefinitionsRequest request = new ValidateFieldDefinitionsRequest();

        //case 1
        Map<String, List<FieldDefinition>> recordsMap = importWorkflowSpec.getFieldDefinitionsRecordsMap();
        Map<String, List> copiedRecordsMap =
                JsonUtils.convertMap(JsonUtils.deserialize(JsonUtils.serialize(recordsMap),
                Map.class), String.class, List.class);
        FieldDefinitionsRecord record = new FieldDefinitionsRecord();
        record.setFieldDefinitionsRecordsMap(recordsMap);
        request.setImportWorkflowSpec(importWorkflowSpec);
        request.setCurrentFieldDefinitionsRecord(record);
        ValidateFieldDefinitionsResponse response = modelingFileMetadataService.validateFieldDefinitions("default",
                "default", "Contacts", "test.csv", request);
        System.out.print(JsonUtils.serialize(response.getFieldValidationMessagesMap()));
        Assert.assertEquals(response.getValidationResult(), ValidateFieldDefinitionsResponse.ValidationResult.PASS);

        // case 2, change field type for lattice field
        Map<String, List<FieldDefinition>> changesMap = new HashMap<>();
        FieldDefinition changedLatticeDefinition = new FieldDefinition();
        changedLatticeDefinition.setFieldType(UserDefinedType.INTEGER);
        changedLatticeDefinition.setFieldName("FirstName");
        changedLatticeDefinition.setColumnName("FirstName");
        changesMap.put("Contact Fields", Arrays.asList(changedLatticeDefinition));
        response = modelingFileMetadataService.validateFieldDefinitions("default",
                "default", "Contacts", "test.csv", request);
        Assert.assertEquals(response.getValidationResult(), ValidateFieldDefinitionsResponse.ValidationResult.ERROR);

        // case 3, change type for customer fields
        FieldDefinition customDefinition = new FieldDefinition();
        customDefinition.setColumnName("custom field1");
        customDefinition.setInCurrentImport(Boolean.TRUE);
        customDefinition.setFieldType(UserDefinedType.INTEGER);
        recordsMap.put("Custom Fields", Arrays.asList(customDefinition));
        Map<String, List<FieldDefinition>> changesMap1 = new HashMap<>();
        FieldDefinition changedCustomDefinition = new FieldDefinition();
        changedCustomDefinition.setColumnName("custom field1");
        changedCustomDefinition.setFieldType(UserDefinedType.TEXT);
        changesMap1.put("Custom Fields", Arrays.asList(changedCustomDefinition));
        response = modelingFileMetadataService.validateFieldDefinitions("default",
                "default", "Contacts", "test.csv", request);
        Assert.assertEquals(response.getValidationResult(), ValidateFieldDefinitionsResponse.ValidationResult.WARNING);
    }
}
