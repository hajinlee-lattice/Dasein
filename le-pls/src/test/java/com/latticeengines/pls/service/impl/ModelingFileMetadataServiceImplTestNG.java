package com.latticeengines.pls.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.closeable.resource.CloseableResourcePool;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.metadata.standardschemas.ImportWorkflowSpec;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinition;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionSectionName;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.service.ModelingFileMetadataService;
import com.latticeengines.pls.util.ImportWorkflowUtilsTestNG;

public class ModelingFileMetadataServiceImplTestNG extends PlsFunctionalTestNGBase {

    private InputStream fileInputStream;

    private static Logger log = LoggerFactory.getLogger(ModelingFileMetadataServiceImplTestNG.class);

    private static String testSpecFileName =
            "com/latticeengines/pls/service/impl/importworkflowspecservice/other-contacts-spec.json";

    private File dataFile;

    @Value("${pls.modelingservice.basedir}")
    private String modelingBaseDir;

    @Autowired
    private ModelingFileMetadataService modelingFileMetadataService;

    @Override
    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
    }

    @Test(groups = "functional")
    public void uploadFileWithMissingRequiredFields() throws Exception {
        dataFile = new File(ClassLoader.getSystemResource(
                "com/latticeengines/pls/service/impl/fileuploadserviceimpl/file_missing_required_fields.csv").getPath());
        fileInputStream = new BufferedInputStream(new FileInputStream(dataFile));
        CloseableResourcePool closeableResourcePool = new CloseableResourcePool();
        boolean thrown = false;
        try {
            modelingFileMetadataService.validateHeaderFields(fileInputStream, closeableResourcePool,
                    dataFile.getName(), true);
            closeableResourcePool.close();
        } catch (Exception e) {
            thrown = true;
            assertTrue(e instanceof LedpException);
            assertEquals(((LedpException) e).getCode(), LedpCode.LEDP_18120);
        } finally {
            closeableResourcePool.close();
            assertTrue(thrown);
        }
    }

    @Test(groups = "functional")
    public void uploadFileWithEmptyHeaderName() throws Exception {
        dataFile = new File(ClassLoader.getSystemResource(
                "com/latticeengines/pls/service/impl/fileuploadserviceimpl/file_empty_header.csv").getPath());
        fileInputStream = new BufferedInputStream(new FileInputStream(dataFile));
        CloseableResourcePool closeableResourcePool = new CloseableResourcePool();
        boolean thrown = false;

        try {
            modelingFileMetadataService.validateHeaderFields(fileInputStream, closeableResourcePool,
                    dataFile.getName(), true);
        } catch (Exception e) {
            thrown = true;
            assertTrue(e instanceof LedpException);
            assertEquals(((LedpException) e).getCode(), LedpCode.LEDP_18096);
        } finally {
            closeableResourcePool.close();
            assertTrue(thrown);
        }
    }

    @Test(groups = "functional")
    public void uploadFileWithUnexpectedCharacterInHeaderName() throws Exception {
        dataFile = new File(ClassLoader.getSystemResource(
                "com/latticeengines/pls/service/impl/fileuploadserviceimpl/file_unexpected_character_in_header.csv")
                .getPath());
        fileInputStream = new BufferedInputStream(new FileInputStream(dataFile));
        CloseableResourcePool closeableResourcePool = new CloseableResourcePool();

        modelingFileMetadataService.validateHeaderFields(fileInputStream, closeableResourcePool, dataFile.getName(),
                true);

        closeableResourcePool.close();

    }

    @Test(groups = "functional")
    public void uploadFileWithDuplicateHeaders1() throws Exception {
        dataFile = new File(ClassLoader.getSystemResource(
                "com/latticeengines/pls/service/impl/fileuploadserviceimpl/file_duplicate_headers1.csv").getPath());
        fileInputStream = new BufferedInputStream(new FileInputStream(dataFile));
        CloseableResourcePool closeableResourcePool = new CloseableResourcePool();
        boolean thrown = false;
        try {
            modelingFileMetadataService.validateHeaderFields(fileInputStream, closeableResourcePool,
                    dataFile.getName(), true);
        } catch (Exception e) {
            thrown = true;
        } finally {
            closeableResourcePool.close();
            assertTrue(thrown);
        }
    }

    @Test(groups = "functional")
    public void uploadFileWithHeadersHavingReservedWords() throws Exception {
        dataFile = new File(ClassLoader.getSystemResource(
                "com/latticeengines/pls/service/impl/fileuploadserviceimpl/file_headers_with_reserved_words.csv").getPath());
        fileInputStream = new BufferedInputStream(new FileInputStream(dataFile));
        CloseableResourcePool closeableResourcePool = new CloseableResourcePool();
        boolean thrown = false;
        try {
            modelingFileMetadataService.validateHeaderFields(fileInputStream, closeableResourcePool,
                    dataFile.getName(), true);
        } catch (Exception e) {
            thrown = true;
            assertTrue(e instanceof LedpException);
            assertEquals(((LedpException) e).getCode(), LedpCode.LEDP_18122);
        } finally {
            closeableResourcePool.close();
            assertTrue(thrown);
        }
    }

    @Test(groups = "functional")
    public void testValidateIndividualSpec() throws Exception {

        ImportWorkflowSpec testSpec = ImportWorkflowUtilsTestNG.pojoFromJsonResourceFile(testSpecFileName,
                ImportWorkflowSpec.class);

        log.error("Expected import workflow spec is:\n" + JsonUtils.pprint(testSpec));
        InputStream specInputStream = new ByteArrayInputStream(JsonUtils.serialize(testSpec).getBytes());

        // case 1: input the same spec in S3
        List<String> errors  = modelingFileMetadataService.validateIndividualSpec("other", "contacts",
                specInputStream);
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
        errors  = modelingFileMetadataService.validateIndividualSpec("other", "contacts",
                specInputStream);
        Assert.assertNotNull(errors);
        Assert.assertTrue(errors.contains("duplicates found in matching column for field name FirstName"));

        // case 3: required flag
        firstNameDefinition.setRequired(null);
        specInputStream = new ByteArrayInputStream(JsonUtils.serialize(testSpec).getBytes());
        errors  = modelingFileMetadataService.validateIndividualSpec("other", "contacts",
                specInputStream);
        Assert.assertNotNull(errors);
        Assert.assertTrue(errors.contains("required flag should be set for FirstName"));

        // case 4: field type change
        firstNameDefinition.setFieldType(UserDefinedType.NUMBER);
        specInputStream = new ByteArrayInputStream(JsonUtils.serialize(testSpec).getBytes());
        errors  = modelingFileMetadataService.validateIndividualSpec("other", "contacts",
                specInputStream);
        Assert.assertNotNull(errors);
        Assert.assertTrue(errors.contains("Physical type TEXT of the FieldDefinition with " +
                "same field name FirstName cannot be changed to NUMBER for system type other and system object " +
                "contacts"));
        Assert.assertTrue(errors.contains("Physical type TEXT of the FieldDefinition with " +
                "same field name FirstName cannot be changed to NUMBER for system type test and system object " +
                "contacts"));

        // case 5: two field definition has same field name
        System.out.println(JsonUtils.pprint(recordsMap));
        List<FieldDefinition> contactFieldDefinitions = recordsMap.get(FieldDefinitionSectionName.Contact_Fields.getName());
        Assert.assertNotNull(contactFieldDefinitions);
        FieldDefinition firstNameDefinition2 = generateFieldDefinition("FirstName", UserDefinedType.TEXT, Arrays.asList(
                "First Name"));
        contactFieldDefinitions.add(firstNameDefinition2);
        specInputStream = new ByteArrayInputStream(JsonUtils.serialize(testSpec).getBytes());
        errors  = modelingFileMetadataService.validateIndividualSpec("other", "contacts",
                specInputStream);
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
