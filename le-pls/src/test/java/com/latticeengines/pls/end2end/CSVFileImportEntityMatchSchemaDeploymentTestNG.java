package com.latticeengines.pls.end2end;

import java.util.HashMap;
import java.util.Map;

import org.joda.time.DateTime;
import org.junit.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class CSVFileImportEntityMatchSchemaDeploymentTestNG extends CSVFileImportDeploymentTestNGBase {

    private static final String ACCOUNT_ENTITY_MATCH_FILE = "Account_base.csv";
    private static final String CONTACT_ENTITY_MATCH_FILE = "Contact_Entity_Match.csv";

    @Override
    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        String featureFlag = LatticeFeatureFlag.ENABLE_ENTITY_MATCH.getName();
        Map<String, Boolean> flags = new HashMap<>();
        flags.put(featureFlag, true);
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.CG, flags);
        MultiTenantContext.setTenant(mainTestTenant);
        customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
    }

    @Test(groups = "deployment")
    public void testContactStandardSchema() {
        Table standardTable = SchemaRepository.instance().getSchema(BusinessEntity.Contact, true, false, true);
        Attribute customerContactId = standardTable.getAttribute(InterfaceName.CustomerContactId);
        Attribute customerAccountId = standardTable.getAttribute(InterfaceName.CustomerAccountId);
        Attribute city = standardTable.getAttribute(InterfaceName.City);
        Attribute companyName = standardTable.getAttribute(InterfaceName.CompanyName);
        Attribute website = standardTable.getAttribute(InterfaceName.Website);
        Attribute contactId = standardTable.getAttribute(InterfaceName.ContactId);
        Attribute accountId = standardTable.getAttribute(InterfaceName.AccountId);
        Assert.assertNotNull(customerContactId);
        Assert.assertNotNull(customerAccountId);
        Assert.assertNotNull(city);
        Assert.assertNotNull(companyName);
        Assert.assertNotNull(website);
        Assert.assertNull(contactId);
        Assert.assertNull(accountId);
    }

    @Test(groups = "deployment")
    public void testContactSchema() {
        SourceFile sourceFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(ENTITY_CONTACT), ENTITY_CONTACT, CONTACT_ENTITY_MATCH_FILE,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + CONTACT_ENTITY_MATCH_FILE));

        String feedType = getFeedTypeByEntity(DEFAULT_SYSTEM, ENTITY_CONTACT);
        FieldMappingDocument fieldMappingDocument = modelingFileMetadataService
                .getFieldMappingDocumentBestEffort(sourceFile.getName(), ENTITY_CONTACT, SOURCE, feedType);
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getMappedField() == null) {
                fieldMapping.setMappedField(fieldMapping.getUserField());
                fieldMapping.setMappedToLatticeField(false);
            }
            if (fieldMapping.getUserField().equals("City")) {
                Assert.assertTrue(fieldMapping.isMappedToLatticeField());
                Assert.assertEquals(fieldMapping.getMappedField(), InterfaceName.City.name());
            }
            if (fieldMapping.getUserField().equals("ID")) {
                Assert.assertTrue(fieldMapping.isMappedToLatticeField());
                Assert.assertEquals(fieldMapping.getMappedField(), InterfaceName.CustomerContactId.name());
            }
            if (fieldMapping.getUserField().equals("Account_ID")) {
                Assert.assertTrue(fieldMapping.isMappedToLatticeField());
                Assert.assertEquals(fieldMapping.getMappedField(), InterfaceName.CustomerAccountId.name());
            }
        }
        modelingFileMetadataService.resolveMetadata(sourceFile.getName(), fieldMappingDocument, ENTITY_CONTACT, SOURCE,
                feedType);
        sourceFile = sourceFileService.findByName(sourceFile.getName());

        String dfIdExtra = cdlService.createS3Template(customerSpace, sourceFile.getName(),
                SOURCE, ENTITY_CONTACT, feedType, null, ENTITY_CONTACT + "Data");

        Assert.assertNotNull(dfIdExtra);
    }

    @Test(groups = "deployment")
    public void testAccountSchema() {
        SourceFile sourceFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(ENTITY_ACCOUNT), ENTITY_ACCOUNT, ACCOUNT_ENTITY_MATCH_FILE,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + ACCOUNT_ENTITY_MATCH_FILE));

        String feedType = getFeedTypeByEntity(DEFAULT_SYSTEM, ENTITY_ACCOUNT);
        FieldMappingDocument fieldMappingDocument = modelingFileMetadataService
                .getFieldMappingDocumentBestEffort(sourceFile.getName(), ENTITY_ACCOUNT, SOURCE, feedType);
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getMappedField() == null) {
                fieldMapping.setMappedField(fieldMapping.getUserField());
                fieldMapping.setMappedToLatticeField(false);
            }
            if (fieldMapping.getUserField().equals("ID")) {
                Assert.assertTrue(fieldMapping.isMappedToLatticeField());
                Assert.assertEquals(fieldMapping.getMappedField(), InterfaceName.CustomerAccountId.name());
            }
        }
        modelingFileMetadataService.resolveMetadata(sourceFile.getName(), fieldMappingDocument, ENTITY_ACCOUNT, SOURCE,
                feedType);
        sourceFile = sourceFileService.findByName(sourceFile.getName());

        String dfIdExtra = cdlService.createS3Template(customerSpace, sourceFile.getName(), SOURCE, ENTITY_ACCOUNT,
                feedType, null, ENTITY_ACCOUNT + "Data");

        Assert.assertNotNull(dfIdExtra);
    }
}
