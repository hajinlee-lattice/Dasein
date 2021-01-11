package com.latticeengines.pls.end2end.fileimport;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.ByteArrayHttpMessageConverter;
import org.springframework.web.client.RestTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.csv.LECSVFormat;
import com.latticeengines.common.exposed.util.TimeStampConvertUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.pls.S3ImportTemplateDisplay;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.pls.frontend.LatticeFieldCategory;
import com.latticeengines.domain.exposed.pls.frontend.LatticeSchemaField;

public class CSVFileImportDateFormatDeploymentTestNG extends CSVFileImportDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(CSVFileImportDateFormatDeploymentTestNG.class);

    private static final String CONTACT_DATE_FILE = "Contact_Date.csv";

    private static final String CUSTOM = "Custom";
    private static final String STANDARD = "Standard";
    private static final String UNMAPPED = "unmapped";
    private RestTemplate restTemplate;

    @BeforeClass(groups = "deployment.import.group2")
    public void setup() throws Exception {
        Map<String, Boolean> flags = new HashMap<>();
        flags.put(LatticeFeatureFlag.ENABLE_ENTITY_MATCH_GA.getName(), true);
        flags.put(LatticeFeatureFlag.ENABLE_ENTITY_MATCH.getName(), false);
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.CG, flags);
        MultiTenantContext.setTenant(mainTestTenant);
        customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
        restTemplate = testBed.getRestTemplate();
    }

    @Test(groups = "deployment.import.group2")
    public void testContactDate() throws IOException {
        baseContactFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(ENTITY_CONTACT), ENTITY_CONTACT, CONTACT_DATE_FILE,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + CONTACT_DATE_FILE));
        String feedType = getFeedTypeByEntity(DEFAULT_SYSTEM, ENTITY_CONTACT);
        List<LatticeSchemaField> latticeSchema =
                modelingFileMetadataService.getSchemaToLatticeSchemaFields(ENTITY_CONTACT, SOURCE, feedType);

        boolean createdDate = false;
        boolean lastModifiedDate = false;
        for (LatticeSchemaField schemaField : latticeSchema) {
            if (schemaField.getName().equals("CreatedDate")) {
                createdDate = true;
                Assert.assertEquals(schemaField.getFieldType(), UserDefinedType.DATE);
                Assert.assertFalse(schemaField.getFromExistingTemplate());
            }
            if (schemaField.getName().equals("LastModifiedDate")) {
                lastModifiedDate = true;
                Assert.assertEquals(schemaField.getFieldType(), UserDefinedType.DATE);
                Assert.assertFalse(schemaField.getFromExistingTemplate());
            }
        }
        Assert.assertTrue(createdDate);
        Assert.assertTrue(lastModifiedDate);
        FieldMappingDocument fieldMappingDocument = modelingFileMetadataService
                .getFieldMappingDocumentBestEffort(baseContactFile.getName(), ENTITY_CONTACT, SOURCE, feedType);
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getUserField().equals("Created Date")) {
                Assert.assertTrue(fieldMapping.isMappedToLatticeField());
                Assert.assertEquals(fieldMapping.getFieldType(), UserDefinedType.DATE);
                Assert.assertEquals(fieldMapping.getDateFormatString(), "MM/DD/YYYY");
            }
        }
        modelingFileMetadataService.resolveMetadata(baseContactFile.getName(), fieldMappingDocument, ENTITY_CONTACT, SOURCE,
                feedType);
        baseContactFile = sourceFileService.findByName(baseContactFile.getName());

        String dfIdExtra = cdlService.createS3Template(customerSpace, baseContactFile.getName(),
                SOURCE, ENTITY_CONTACT, feedType, null, ENTITY_CONTACT + "Data");
        Assert.assertNotNull(baseContactFile);
        Assert.assertNotNull(dfIdExtra);

        SourceFile newContactFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(ENTITY_CONTACT), ENTITY_CONTACT, CONTACT_SOURCE_FILE,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + CONTACT_SOURCE_FILE));
        latticeSchema = modelingFileMetadataService.getSchemaToLatticeSchemaFields(ENTITY_CONTACT, SOURCE, feedType);
        createdDate = false;
        lastModifiedDate = false;
        for (LatticeSchemaField schemaField : latticeSchema) {
            if (schemaField.getName().equals("CreatedDate")) {
                createdDate = true;
                Assert.assertEquals(schemaField.getFieldType(), UserDefinedType.DATE);
                Assert.assertTrue(schemaField.getFromExistingTemplate());
            }
            if (schemaField.getName().equals("LastModifiedDate")) {
                lastModifiedDate = true;
            }
        }
        Assert.assertTrue(createdDate);
        Assert.assertFalse(lastModifiedDate);

        fieldMappingDocument = modelingFileMetadataService
                .getFieldMappingDocumentBestEffort(newContactFile.getName(), ENTITY_CONTACT, SOURCE, feedType);
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getUserField().equals("Created Date")) {
                Assert.assertTrue(fieldMapping.isMappedToLatticeField());
                Assert.assertEquals(fieldMapping.getFieldType(), UserDefinedType.DATE);
                Assert.assertEquals(fieldMapping.getDateFormatString(), "MM/DD/YYYY");
            } else if (fieldMapping.getUserField().equals("LastModifiedDate")) {
                Assert.assertFalse(fieldMapping.isMappedToLatticeField());
                Assert.assertEquals(fieldMapping.getFieldType(), UserDefinedType.DATE);
                Assert.assertEquals(fieldMapping.getTimeFormatString(), "00:00:00 12H");
            }
        }


        restTemplate.getMessageConverters().add(new ByteArrayHttpMessageConverter());
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Arrays.asList(MediaType.ALL));
        headers.setContentType(MediaType.APPLICATION_JSON);
        S3ImportTemplateDisplay templateDisplay = new S3ImportTemplateDisplay();
        templateDisplay.setFeedType(feedType);
        ObjectMapper mapper = new ObjectMapper();
        String payload = mapper.writeValueAsString(templateDisplay);
        HttpEntity<String> entity = new HttpEntity<>(payload, headers);
        ResponseEntity<byte[]> response = restTemplate.exchange(
                String.format("%s/pls/cdl/s3import/template/downloadcsv", getRestAPIHostPort()), HttpMethod.POST,
                entity, byte[].class);
        assertEquals(response.getStatusCode(), HttpStatus.OK);
        String fileContent = new String(response.getBody());
        Assert.assertNotNull(fileContent);
        String[] mappings = fileContent.split("\n");
        boolean firstLine = true;
        for (String mapping : mappings) {
            if (firstLine) {
                firstLine = false;
                assertTemplateMappingHeaders(mapping);
            } else {
                String[] fields = mapping.split(",");
                if (fields[3].equals("ContactId")) {
                    Assert.assertEquals(fields[0], CUSTOM);
                    Assert.assertEquals(fields[2], "ID");
                    Assert.assertEquals(fields[4], UserDefinedType.TEXT.name());
                } else if (fields[3].equals("CustomerContactId")) {
                    Assert.assertEquals(fields[0], STANDARD);
                    Assert.assertEquals(fields[1], LatticeFieldCategory.Other.name());
                    Assert.assertEquals(fields[2], "ID");
                    Assert.assertEquals(fields[4], UserDefinedType.TEXT.name());
                } else if (fields[3].equals("ContactName")) {
                    Assert.assertEquals(fields[0], STANDARD);
                    Assert.assertEquals(fields[1], LatticeFieldCategory.Other.name());
                    Assert.assertEquals(fields[2], "Name");
                    Assert.assertEquals(fields[4], UserDefinedType.TEXT.name());
                } else if (fields[3].equals("CreatedDate")) {
                    Assert.assertEquals(fields[0], STANDARD);
                    Assert.assertEquals(fields[1], LatticeFieldCategory.Other.name());
                    Assert.assertEquals(fields[2], "Created Date");
                    Assert.assertEquals(fields[4], "MM/DD/YYYY 00:00:00 12H");
                } else if (fields[3].equals("LastModifiedDate")) {
                    Assert.assertEquals(fields[0], STANDARD);
                    Assert.assertEquals(fields[1], LatticeFieldCategory.Other.name());
                    Assert.assertEquals(fields[2], UNMAPPED);
                    Assert.assertEquals(fields[4], UserDefinedType.DATE.name());
                }

            }
        }
    }

    private void assertTemplateMappingHeaders(String mapping) {
        String[] fields = mapping.split(",");
        Assert.assertEquals(fields.length, 5);
        Assert.assertEquals(fields[0], "Field Type");
        Assert.assertEquals(fields[1], "Category");
        Assert.assertEquals(fields[2], "Your Field Name");
        Assert.assertEquals(fields[3], "Standard Field Name");
        Assert.assertEquals(fields[4], "Data Type");
    }


    @Test(groups = "deployment.import.group2")
    public void testAccountDateFormat() throws IOException {
        baseAccountFile = uploadSourceFile(ACCOUNT_SOURCE_FILE, ENTITY_ACCOUNT);
        String dateFormatString1 = "DD/MM/YYYY";
        String timezone1 = "America/New_York";
        String dateFormatString2 = "MM.DD.YY";
        String timeFormatString2 = "00:00:00 24H";
        String timezone2 = "Asia/Shanghai";
        String storedDateFormatString1 = "MM/DD/YYYY";
        String storedTimeFormatString2 = "00:00:00 12H";

        Assert.assertNotNull(baseAccountFile);

        String dfId = cdlService.createS3Template(customerSpace, baseAccountFile.getName(),
                SOURCE, ENTITY_ACCOUNT, getFeedTypeByEntity(DEFAULT_SYSTEM, ENTITY_ACCOUNT), null, ENTITY_ACCOUNT +
                        "Data");

        SourceFile accountDateSF = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(ENTITY_ACCOUNT), ENTITY_ACCOUNT, ACCOUNT_SOURCE_FILE_FROMATDATE,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + ACCOUNT_SOURCE_FILE_FROMATDATE));

        String feedType = getFeedTypeByEntity(DEFAULT_SYSTEM, ENTITY_ACCOUNT);
        FieldMappingDocument fieldMappingDocument = modelingFileMetadataService
                .getFieldMappingDocumentBestEffort(accountDateSF.getName(), ENTITY_ACCOUNT, SOURCE, feedType);
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getUserField().equals("TestDate1")) {
                Assert.assertEquals(fieldMapping.getFieldType(), UserDefinedType.DATE);
                Assert.assertEquals(fieldMapping.getDateFormatString(), dateFormatString1);
                // change the auto detection result for date pattern
                fieldMapping.setFieldType(UserDefinedType.DATE);
                fieldMapping.setMappedToLatticeField(false);
                fieldMapping.setDateFormatString(storedDateFormatString1);
                fieldMapping.setTimezone(timezone1);
            } else if (fieldMapping.getUserField().equals("TestDate2")) {
                Assert.assertEquals(fieldMapping.getFieldType(), UserDefinedType.DATE);
                Assert.assertEquals(fieldMapping.getDateFormatString(), dateFormatString2);
                Assert.assertEquals(fieldMapping.getTimeFormatString(), timeFormatString2);
                // change the auto detection result for time pattern
                fieldMapping.setFieldType(UserDefinedType.DATE);
                fieldMapping.setMappedToLatticeField(false);
                fieldMapping.setDateFormatString(dateFormatString2);
                fieldMapping.setTimeFormatString(storedTimeFormatString2);
                fieldMapping.setTimezone(timezone2);
            }
        }

        modelingFileMetadataService.resolveMetadata(accountDateSF.getName(), fieldMappingDocument, ENTITY_ACCOUNT, SOURCE,
                feedType);
        accountDateSF = sourceFileService.findByName(accountDateSF.getName());

        String dfIdExtra = cdlService.createS3Template(customerSpace, accountDateSF.getName(),
                SOURCE, ENTITY_ACCOUNT, feedType, null, ENTITY_ACCOUNT + "Data");

        fieldMappingDocument = modelingFileMetadataService
                .getFieldMappingDocumentBestEffort(accountDateSF.getName(), ENTITY_ACCOUNT, SOURCE, feedType);
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getUserField().equals("TestDate1")) {
                Assert.assertEquals(fieldMapping.getDateFormatString(), storedDateFormatString1);
                Assert.assertEquals(fieldMapping.getTimezone(), timezone1);
            } else if (fieldMapping.getUserField().equals("TestDate2")) {
                Assert.assertEquals(fieldMapping.getDateFormatString(), dateFormatString2);
                Assert.assertEquals(fieldMapping.getTimeFormatString(), storedTimeFormatString2);
                Assert.assertEquals(fieldMapping.getTimezone(), timezone2);
            }
        }

        Assert.assertEquals(dfId, dfIdExtra);

        restTemplate.getMessageConverters().add(new ByteArrayHttpMessageConverter());
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Arrays.asList(MediaType.ALL));
        headers.setContentType(MediaType.APPLICATION_JSON);
        S3ImportTemplateDisplay templateDisplay = new S3ImportTemplateDisplay();
        templateDisplay.setFeedType(feedType);
        ObjectMapper mapper = new ObjectMapper();
        String payload = mapper.writeValueAsString(templateDisplay);
        HttpEntity<String> entity = new HttpEntity<>(payload, headers);
        ResponseEntity<byte[]> response = restTemplate.exchange(
                String.format("%s/pls/cdl/s3import/template/downloadcsv", getRestAPIHostPort()), HttpMethod.POST,
                entity, byte[].class);
        assertEquals(response.getStatusCode(), HttpStatus.OK);
        String results = new String(response.getBody());
        String fileName = response.getHeaders().getFirst("Content-Disposition");
        assertTrue(fileName.contains(".csv"));
        assertTrue(fileName.contains("template_DefaultSystem_AccountData"));
        assertTrue(results.length() > 0);
        CSVParser parser = null;
        InputStream is = new ByteArrayInputStream(response.getBody());

        InputStreamReader reader = new InputStreamReader(is);
        CSVFormat format = LECSVFormat.format;
        try {
            parser = new CSVParser(reader, format);
            Set<String> csvHeaders = parser.getHeaderMap().keySet();
            assertTrue(csvHeaders.contains("Field Type"));
            assertTrue(csvHeaders.contains("Category"));
            assertTrue(csvHeaders.contains("Your Field Name"));
            assertTrue(csvHeaders.contains("Standard Field Name"));
            assertTrue(csvHeaders.contains("Data Type"));
            for (CSVRecord record : parser.getRecords()) {
                verifyAccountMapping(record.get("Field Type"), record.get("Category"), record.get("Your Field Name"),
                        record.get("Standard Field Name"), record.get("Data Type"));
            }
        } catch (Exception e) {
            log.warn("failed to read csv", e);
        } finally {
            parser.close();
        }
    }

    private void verifyAccountMapping(String field0, String field1, String field2, String field3, String field4) {
        if ("AccountId".equals(field3)) {
            Assert.assertEquals(field0, STANDARD);
//            Assert.assertEquals(field1, LatticeFieldCategory.MatchField.name());
            Assert.assertEquals(field2, "ID");
            Assert.assertEquals(field4, UserDefinedType.TEXT.name());
        } else if ("CustomerAccountId".equals(field3)) {
            Assert.assertEquals(field0, STANDARD);
            Assert.assertEquals(field1, LatticeFieldCategory.Other.name());
            Assert.assertEquals(field2, "ID");
            Assert.assertEquals(field4, UserDefinedType.TEXT.name());
        } else if ("Type".equals(field3)) {
            Assert.assertEquals(field0, STANDARD);
            Assert.assertEquals(field1, LatticeFieldCategory.Other.name());
            Assert.assertEquals(field2, "Type");
            Assert.assertEquals(field4, UserDefinedType.TEXT.name());
        } else if ("user_TestDate1".equalsIgnoreCase(field3)) {
            Assert.assertEquals(field0, CUSTOM);
            Assert.assertTrue(StringUtils.isBlank(field1));
            Assert.assertEquals(field2, "TestDate1");
            Assert.assertEquals(field4, "MM/DD/YYYY America/New_York");
        } else if ("user_TestDate2".equalsIgnoreCase(field3)) {
            Assert.assertEquals(field0, CUSTOM);
            Assert.assertTrue(StringUtils.isBlank(field1));
            Assert.assertEquals(field2, "TestDate2");
            Assert.assertEquals(field4, "MM.DD.YY 00:00:00 12H Asia/Shanghai");
        } else if ("user_TestDate3".equalsIgnoreCase(field3)) {
            Assert.assertEquals(field0, CUSTOM);
            Assert.assertTrue(StringUtils.isBlank(field1));
            Assert.assertEquals(field2, "TestDate3");
            Assert.assertEquals(field4, "YYYY-MMM-DD 00:00 12H " + TimeStampConvertUtils.SYSTEM_USER_TIME_ZONE);
        } else if ("LastModifiedDate".equals(field3)) {
            Assert.assertEquals(field0, STANDARD);
            Assert.assertEquals(field2, UNMAPPED);
            Assert.assertEquals(field4, UserDefinedType.DATE.name());
        }
    }
}
