package com.latticeengines.pls.end2end2;

import java.util.List;

import org.joda.time.DateTime;
import org.springframework.web.client.RestTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.TimeStampConvertUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FetchFieldDefinitionsResponse;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinition;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionSectionName;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;
import com.latticeengines.domain.exposed.pls.frontend.LatticeSchemaField;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.query.EntityTypeUtils;

public class CSVFileImportDateFormatDeploymentIW2TestNG extends CSVFileImportDeploymentIW2TestNGBase {
    private static final String CONTACT_DATE_FILE = "Contact_Date.csv";

    private static final String CUSTOM = "Custom";
    private static final String STANDARD = "Standard";
    private static final String UNMAPPED = "unmapped";
    private RestTemplate restTemplate;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.CG);
        MultiTenantContext.setTenant(mainTestTenant);
        customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
        restTemplate = testBed.getRestTemplate();
    }

    @Test(groups = "deployment")
    public void testContactDate() throws Exception {
        baseContactFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                EntityType.Contacts.getSchemaInterpretation(), EntityType.Contacts.getEntity().name(),
                CONTACT_DATE_FILE, ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + CONTACT_DATE_FILE));
        String feedType = EntityTypeUtils.generateFullFeedType(DEFAULT_SYSTEM, EntityType.Contacts);
        List<LatticeSchemaField> latticeSchema =
                modelingFileMetadataService.getSchemaToLatticeSchemaFields(EntityType.Contacts.getEntity().name(),
                        SOURCE, feedType);

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
        FetchFieldDefinitionsResponse fetchFieldDefinitionsResponse = dataMappingService.fetchFieldDefinitions(
                DEFAULT_SYSTEM, DEFAULT_SYSTEM_TYPE, EntityType.Contacts.getDisplayName(), baseContactFile.getName());

        FieldDefinitionsRecord fieldDefinitionsRecord =
                fetchFieldDefinitionsResponse.getCurrentFieldDefinitionsRecord();

        for (FieldDefinition definition :
                fieldDefinitionsRecord.getFieldDefinitionsRecords(FieldDefinitionSectionName.Analysis_Fields.getName())) {
            if ("Created Date".equals(definition.getColumnName())) {
                Assert.assertTrue(definition.isInCurrentImport());
                Assert.assertEquals(definition.getFieldType(), UserDefinedType.DATE);
                Assert.assertEquals(definition.getDateFormat(), "MM/DD/YYYY");
            }
        }
        dataMappingService.commitFieldDefinitions(DEFAULT_SYSTEM,  DEFAULT_SYSTEM_TYPE,
                EntityType.Contacts.getDisplayName(), baseContactFile.getName(), false, fieldDefinitionsRecord);
        baseContactFile = sourceFileService.findByName(baseContactFile.getName());

        Assert.assertNotNull(baseContactFile);


        latticeSchema = modelingFileMetadataService.getSchemaToLatticeSchemaFields(EntityType.Contacts.getEntity().name(),
                SOURCE,
                feedType);
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

        SourceFile newContactFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                EntityType.Contacts.getSchemaInterpretation(), EntityType.Contacts.getEntity().name(), CONTACT_SOURCE_FILE,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + CONTACT_SOURCE_FILE));
        fetchFieldDefinitionsResponse = dataMappingService.fetchFieldDefinitions(DEFAULT_SYSTEM,
                DEFAULT_SYSTEM_TYPE, EntityType.Contacts.getDisplayName(), newContactFile.getName());
        fieldDefinitionsRecord =
                fetchFieldDefinitionsResponse.getCurrentFieldDefinitionsRecord();

        for (FieldDefinition definition :
                fieldDefinitionsRecord.getFieldDefinitionsRecords(FieldDefinitionSectionName.Analysis_Fields.getName())) {
            if ("Created Date".equals(definition.getColumnName())) {
                Assert.assertEquals(definition.getFieldType(), UserDefinedType.DATE);
                Assert.assertEquals(definition.getDateFormat(), "MM/DD/YYYY");
            } else if ("LastModifiedDate".equals(definition.getColumnName())) {
                Assert.assertEquals(definition.getFieldType(), UserDefinedType.DATE);
                Assert.assertEquals(definition.getTimeFormat(), "00:00:00 12H");
            }
        }

        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace, SOURCE, feedType);
        Table standardTable = SchemaRepository.instance().getSchema(BusinessEntity.Contact, true, false, true);
        String fileContent = cdlService.getTemplateMappingContent(dataFeedTask.getImportTemplate(), standardTable);
        Assert.assertNotNull(fileContent);
        String[] mappings = fileContent.split("\n");
        boolean firstLine = true;
        for (String mapping : mappings) {
            if (firstLine) {
                firstLine = false;
                assertTemplateMappingHeaders(mapping);
            } else {
                String[] fields = mapping.split(",");
                if (fields[2].equals("ContactId")) {
                    Assert.assertEquals(fields[0], CUSTOM);
                    //Assert.assertEquals(fields[1], "ID");
                    Assert.assertEquals(fields[3], UserDefinedType.TEXT.name());
                } else if (fields[2].equals("CustomerContactId")) {
                    Assert.assertEquals(fields[0], STANDARD);
                    //Assert.assertEquals(fields[1], "ID");
                    Assert.assertEquals(fields[3], UserDefinedType.TEXT.name());
                } else if (fields[2].equals("ContactName")) {
                    Assert.assertEquals(fields[0], STANDARD);
                   // Assert.assertEquals(fields[1], "Name");
                    Assert.assertEquals(fields[3], UserDefinedType.TEXT.name());
                } else if (fields[2].equals("CreatedDate")) {
                    Assert.assertEquals(fields[0], STANDARD);
                    Assert.assertEquals(fields[1], "Created Date");
                    Assert.assertEquals(fields[3], "MM/DD/YYYY 00:00:00 12H");
                } else if (fields[2].equals("LastModifiedDate")) {
                    Assert.assertEquals(fields[0], STANDARD);
                    Assert.assertEquals(fields[1], UNMAPPED);
                    Assert.assertEquals(fields[3], UserDefinedType.DATE.name());
                }

            }
        }
    }

    private void assertTemplateMappingHeaders(String mapping) {
        String[] fields = mapping.split(",");
        Assert.assertEquals(fields.length, 4);
        Assert.assertEquals(fields[0], "Field Type");
        Assert.assertEquals(fields[1], "Your Field Name");
        Assert.assertEquals(fields[2], "Lattice Field Name");
        Assert.assertEquals(fields[3], "Data Type");
    }


    @Test(groups = "deployment")
    public void testAccountDateFormat() throws Exception {
        baseAccountFile = uploadSourceFile(DEFAULT_SYSTEM, DEFAULT_SYSTEM_TYPE, EntityType.Accounts,
                ACCOUNT_SOURCE_FILE);
        String dateFormatString1 = "DD/MM/YYYY";
        String timezone1 = "America/New_York";
        String dateFormatString2 = "MM.DD.YY";
        String timeFormatString2 = "00:00:00 24H";
        String timezone2 = "Asia/Shanghai";
        String storedDateFormatString1 = "MM/DD/YYYY";
        String storedTimeFormatString2 = "00:00:00 12H";

        Assert.assertNotNull(baseAccountFile);
        String feedType = EntityTypeUtils.generateFullFeedType(DEFAULT_SYSTEM, EntityType.Accounts);
        String dfId = cdlService.createS3Template(customerSpace, baseAccountFile.getName(),
                SOURCE, EntityType.Accounts.getEntity().name(), feedType, null, feedType);

        SourceFile accountDateSF = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                EntityType.Accounts.getSchemaInterpretation(), EntityType.Accounts.getEntity().name(),
                ACCOUNT_SOURCE_FILE_FROMATDATE,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + ACCOUNT_SOURCE_FILE_FROMATDATE));

        FetchFieldDefinitionsResponse fetchFieldDefinitionsResponse =
                dataMappingService.fetchFieldDefinitions(DEFAULT_SYSTEM,
                        DEFAULT_SYSTEM_TYPE, EntityType.Accounts.getDisplayName(), accountDateSF.getName());
        FieldDefinitionsRecord currentRecord = fetchFieldDefinitionsResponse.getCurrentFieldDefinitionsRecord();
        for (FieldDefinition definition :
                currentRecord.getFieldDefinitionsRecords(FieldDefinitionSectionName.Custom_Fields.getName())) {
            if (definition.getColumnName().equals("TestDate1")) {
                Assert.assertEquals(definition.getFieldType(), UserDefinedType.DATE);
                Assert.assertEquals(definition.getDateFormat(), dateFormatString1);
                // change the auto detection result for date pattern
                definition.setFieldType(UserDefinedType.DATE);
                definition.setDateFormat(storedDateFormatString1);
                definition.setTimeZone(timezone1);
            } else if (definition.getColumnName().equals("TestDate2")) {
                Assert.assertEquals(definition.getFieldType(), UserDefinedType.DATE);
                Assert.assertEquals(definition.getDateFormat(), dateFormatString2);
                Assert.assertEquals(definition.getTimeFormat(), timeFormatString2);
                // change the auto detection result for time pattern
                definition.setFieldType(UserDefinedType.DATE);
                definition.setDateFormat(dateFormatString2);
                definition.setTimeFormat(storedTimeFormatString2);
                definition.setTimeZone(timezone2);
            }
        }
        dataMappingService.commitFieldDefinitions(DEFAULT_SYSTEM,  DEFAULT_SYSTEM_TYPE,
                EntityType.Accounts.getDisplayName(), accountDateSF.getName(), false, currentRecord);
        accountDateSF = sourceFileService.findByName(accountDateSF.getName());

        String dfIdExtra = cdlService.createS3Template(customerSpace, accountDateSF.getName(),
                SOURCE, EntityType.Accounts.getEntity().name(), feedType, null, feedType);

        fetchFieldDefinitionsResponse = dataMappingService.fetchFieldDefinitions(DEFAULT_SYSTEM,
                DEFAULT_SYSTEM_TYPE, EntityType.Accounts.getDisplayName(), accountDateSF.getName());
        currentRecord = fetchFieldDefinitionsResponse.getCurrentFieldDefinitionsRecord();
        for (FieldDefinition fieldDefinition :
                currentRecord.getFieldDefinitionsRecords(FieldDefinitionSectionName.Custom_Fields.getName())) {
            if (fieldDefinition.getColumnName().equals("TestDate1")) {
                Assert.assertEquals(fieldDefinition.getDateFormat(), storedDateFormatString1);
                Assert.assertEquals(fieldDefinition.getTimeZone(), timezone1);
            } else if (fieldDefinition.getColumnName().equals("TestDate2")) {
                Assert.assertEquals(fieldDefinition.getDateFormat(), dateFormatString2);
                Assert.assertEquals(fieldDefinition.getTimeFormat(), storedTimeFormatString2);
                Assert.assertEquals(fieldDefinition.getTimeZone(), timezone2);
            }
        }

        Assert.assertEquals(dfId, dfIdExtra);
//        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace, SOURCE, feedType);
//        Table standardTable = SchemaRepository.instance().getSchema(BusinessEntity.Account, true, false, true);
//        String fileContent = cdlService.getTemplateMappingContent(dataFeedTask.getImportTemplate(), standardTable);
//
//        Assert.assertNotNull(fileContent);
//        String[] mappings = fileContent.split("\n");
//        boolean firstLine = true;
//        for (String mapping : mappings) {
//            if (firstLine) {
//                firstLine = false;
//                assertTemplateMappingHeaders(mapping);
//            } else {
//                String[] fields = mapping.split(",");
//                verifyAccountMapping(fields[0], fields[1], fields[2], fields[3]);
//            }
//        }
//        restTemplate.getMessageConverters().add(new ByteArrayHttpMessageConverter());
//        HttpHeaders headers = new HttpHeaders();
//        headers.setAccept(Arrays.asList(MediaType.ALL));
//        headers.setContentType(MediaType.APPLICATION_JSON);
//        S3ImportTemplateDisplay templateDisplay = new S3ImportTemplateDisplay();
//        templateDisplay.setFeedType("DefaultSystem_AccountData");
//        ObjectMapper mapper = new ObjectMapper();
//        String payload = mapper.writeValueAsString(templateDisplay);
//        HttpEntity<String> entity = new HttpEntity<>(payload, headers);
//        ResponseEntity<byte[]> response = restTemplate.exchange(
//                String.format("%s/pls/cdl/s3import/template/downloadcsv", getRestAPIHostPort()), HttpMethod.POST,
//                entity, byte[].class);
//        assertEquals(response.getStatusCode(), HttpStatus.OK);
//        String results = new String(response.getBody());
//        String fileName = response.getHeaders().getFirst("Content-Disposition");
//        assertTrue(fileName.contains(".csv"));
//        assertTrue(fileName.contains("template_DefaultSystem_AccountData"));
//        assertTrue(results.length() > 0);
//        CSVParser parser = null;
//        InputStream is = new ByteArrayInputStream(response.getBody());
//
//        InputStreamReader reader = new InputStreamReader(is);
//        CSVFormat format = LECSVFormat.format;
//        try {
//            parser = new CSVParser(reader, format);
//            Set<String> csvHeaders = parser.getHeaderMap().keySet();
//            assertTrue(csvHeaders.contains("Field Type"));
//            assertTrue(csvHeaders.contains("Your Field Name"));
//            assertTrue(csvHeaders.contains("Lattice Field Name"));
//            assertTrue(csvHeaders.contains("Data Type"));
//            for (CSVRecord record : parser.getRecords()) {
//                verifyAccountMapping(record.get("Field Type"), record.get("Your Field Name"),
//                        record.get("Lattice Field Name"), record.get("Data Type"));
//            }
//        } catch (Exception e) {
//            // unexpected exception happened
//        } finally {
//            parser.close();
//        }
    }

    private void verifyAccountMapping(String field0, String field1, String field2, String field3) {
        if ("AccountId".equals(field2)) {
            Assert.assertEquals(field0, STANDARD);
            Assert.assertEquals(field1, "ID");
            Assert.assertEquals(field3, UserDefinedType.TEXT.name());
        } else if ("CustomerAccountId".equals(field2)) {
            Assert.assertEquals(field0, STANDARD);
            Assert.assertEquals(field1, "ID");
            Assert.assertEquals(field3, UserDefinedType.TEXT.name());
        } else if ("Type".equals(field2)) {
            Assert.assertEquals(field0, STANDARD);
            Assert.assertEquals(field1, "Type");
            Assert.assertEquals(field3, UserDefinedType.TEXT.name());
        } else if ("user_TestDate1".equals(field2)) {
            Assert.assertEquals(field0, CUSTOM);
            Assert.assertEquals(field1, "TestDate1");
            Assert.assertEquals(field3, "MM/DD/YYYY America/New_York");
        } else if ("user_TestDate2".equals(field2)) {
            Assert.assertEquals(field0, CUSTOM);
            Assert.assertEquals(field1, "TestDate2");
            Assert.assertEquals(field3, "MM.DD.YY 00:00:00 12H Asia/Shanghai");
        } else if ("user_TestDate3".equals(field2)) {
            Assert.assertEquals(field0, CUSTOM);
            Assert.assertEquals(field1, "TestDate3");
            Assert.assertEquals(field3, "YYYY-MMM-DD 00:00 12H " + TimeStampConvertUtils.SYSTEM_USER_TIME_ZONE);
        } else if ("LastModifiedDate".equals(field2)) {
            Assert.assertEquals(field0, STANDARD);
            Assert.assertEquals(field1, UNMAPPED);
            Assert.assertEquals(field3, UserDefinedType.DATE.name());
        }
    }
}
