package com.latticeengines.pls.service.impl;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.metadata.standardschemas.ImportWorkflowSpec;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinition;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionSectionName;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.pls.service.ImportWorkflowService;
import com.latticeengines.proxy.exposed.core.ImportWorkflowSpecProxy;

public class ImportWorkflowServiceDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ImportWorkflowServiceDeploymentTestNG.class);
    @Inject
    private ImportWorkflowSpecProxy importWorkflowSpecProxy;

    @Inject
    private ImportWorkflowService importWorkflowService;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.CG);
        MultiTenantContext.setTenant(mainTestTenant);
    }

    @Test(groups = "deployment")
    public void testValidateIndividualSpec() throws Exception {

        String tenantId = MultiTenantContext.getShortTenantId();
        ImportWorkflowSpec testSpec = importWorkflowSpecProxy.getImportWorkflowSpec(tenantId, "other", "contacts");

        log.error("Expected import workflow spec is:\n" + JsonUtils.pprint(testSpec));
        InputStream specInputStream = new ByteArrayInputStream(JsonUtils.serialize(testSpec).getBytes());
        List<String> errors  = new ArrayList<>();
        // case 1: input the same spec in S3
        importWorkflowService.validateIndividualSpec("other", "contacts", specInputStream, errors);
        Assert.assertNotNull(errors);
        Assert.assertTrue(errors.contains("input spec matches the existing spec with system type other and " +
                "system object contacts"));

        Map<String, List<FieldDefinition>> recordsMap = testSpec.getFieldDefinitionsRecordsMap();
        Assert.assertNotNull(recordsMap);
        Map<String, FieldDefinition> fieldNameToDefinition =
                recordsMap.values().stream().flatMap(List::stream).collect(Collectors.toMap(FieldDefinition::getFieldName,
                        e -> e));

        // case 2: field definition has same matching column name
        FieldDefinition firstNameDefinition = fieldNameToDefinition.get("FirstName");
        firstNameDefinition.setMatchingColumnNames(Arrays.asList("First Name", "First Name"));
        specInputStream = new ByteArrayInputStream(JsonUtils.serialize(testSpec).getBytes());
        importWorkflowService.validateIndividualSpec("other", "contacts", specInputStream, errors);
        Assert.assertNotNull(errors);
        Assert.assertTrue(errors.contains("duplicates found in matching column for field name FirstName"));

        // case 2.b duplicate column name across the field definition
        FieldDefinition lastNameDefinition = fieldNameToDefinition.get("LastName");
        lastNameDefinition.setMatchingColumnNames(Arrays.asList("First Name", "LAST NAME"));
        specInputStream = new ByteArrayInputStream(JsonUtils.serialize(testSpec).getBytes());
        importWorkflowService.validateIndividualSpec("other", "contacts", specInputStream, errors);
        Assert.assertNotNull(errors);
        Assert.assertTrue(errors.contains("duplicates found in matching column for field name LastName"));

        // case 3: required flag
        firstNameDefinition.setRequired(null);
        specInputStream = new ByteArrayInputStream(JsonUtils.serialize(testSpec).getBytes());
        importWorkflowService.validateIndividualSpec("other", "contacts", specInputStream, errors);
        Assert.assertNotNull(errors);
        Assert.assertTrue(errors.contains("required flag should be set for FirstName"));

        // case 4: field type change
        firstNameDefinition.setFieldType(UserDefinedType.NUMBER);
        specInputStream = new ByteArrayInputStream(JsonUtils.serialize(testSpec).getBytes());
        importWorkflowService.validateIndividualSpec("other", "contacts", specInputStream, errors);
        Assert.assertNotNull(errors);
        Assert.assertTrue(errors.contains("Physical type TEXT of the FieldDefinition with " +
                "same field name FirstName cannot be changed to NUMBER for system type other and system object " +
                "contacts"));
        Assert.assertTrue(errors.contains("Physical type NUMBER of the field name FirstName in the current " +
                "systemType other and systemObject contacts cannot be different than the Physical " +
                "type TEXT in other template with system type test and system object contacts"));

        // case 5: two field definition has same field name
        System.out.println(JsonUtils.pprint(recordsMap));
        List<FieldDefinition> contactFieldDefinitions = recordsMap.get(FieldDefinitionSectionName.Contact_Fields.getName());
        Assert.assertNotNull(contactFieldDefinitions);
        FieldDefinition firstNameDefinition2 = generateFieldDefinition("FirstName", UserDefinedType.TEXT,
                Collections.singletonList("First Name"));
        contactFieldDefinitions.add(firstNameDefinition2);
        specInputStream = new ByteArrayInputStream(JsonUtils.serialize(testSpec).getBytes());
        importWorkflowService.validateIndividualSpec("other", "contacts", specInputStream, errors);
        Assert.assertNotNull(errors);
        Assert.assertTrue(errors.contains("field definitions have same field name FirstName"));

    }

    private FieldDefinition generateFieldDefinition(String fieldName, UserDefinedType type, List<String> matchingColumns) {
        FieldDefinition definition = new FieldDefinition();
        definition.setFieldType(type);
        definition.setMatchingColumnNames(matchingColumns);
        definition.setFieldName(fieldName);
        definition.setRequired(true);
        return definition;
    }
}
