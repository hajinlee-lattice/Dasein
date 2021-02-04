package com.latticeengines.apps.cdl.qaend2end.activitystore;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import javax.inject.Inject;

import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableSet;
import com.latticeengines.apps.cdl.testframework.CDLQATestNGBase;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.cdl.DeleteRequest;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.cdl.ProcessAnalyzeRequest;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigRequest;
import com.latticeengines.redshiftdb.exposed.service.RedshiftPartitionService;
import com.latticeengines.testframework.exposed.proxy.pls.PlsCDLSoftDeleteProxy;
import com.latticeengines.testframework.exposed.utils.S3Utilities;
import com.latticeengines.testframework.exposed.utils.Util;

public class ActivityStoreQAEnd2EndTestNG extends CDLQATestNGBase {
    private static final Logger log = LoggerFactory.getLogger(ActivityStoreQAEnd2EndTestNG.class);
    private static final Integer DEFAULT_WAIT_PA_READY_IN_MINUTES = 60;

    private String activityStoreTestDataPath;

    private static final String OTHER_SYSTEM_NAME = "OtherSystem";

    private static final String DEFAULT_ACCOUNT_FILE_NAME = "Account_Default.csv";
    private static final String OTHER_ACCOUNT_FILE_NAME = "Account_Other.csv";

    private static final String DEFAULT_CONTACT_FILE_NAME = "Contacts_Default.csv";

    private static final String INTENT_FILE_NAME = "Intent_Activity_delete.csv";
    private static final String INTENT_TEMPLATE_PATH = "%s/dropfolder/%s/Templates/Default_DnbIntent_System_DnbIntentData";

    private static final String WEBVISIT_FILE_NAME = "WebVisitData_delete.csv";
    private static final String WEBVISIT_SYSTEM = "Default_Website_System";

    private static final String OPPORTUNITY_FILE_NAME = "Opportunity_Data_delete.csv";

    private static final String MARKETING_FILE_NAME = "Marketing_Activity_Data_Eloqua_delete.csv";

    private static final String ACCOUNT_REIMPORT_FILE_NAME = "Account_Re-import.csv";

    private static final String CONTACT_REIMPORT_FILE_NAME = "Contacts_Re-import.csv";

    @Inject
    private RedshiftPartitionService redshiftPartitionService;

    private JdbcTemplate redshiftJdbcTemplate;

    private List<Map<String, Object>> allAccounts = null;

    private List<Map<String, Object>> allContacts = null;

    @Inject
    private PlsCDLSoftDeleteProxy plsCDLSoftDeleteProxy;

    @BeforeClass(groups = "qa-activitystore-end2end")
    public void init() {
        super.init();
        downloadTestData();
        Assert.assertTrue(StringUtils.isNotEmpty(qaTestDataPath), "Test Data directory is required");
        activityStoreTestDataPath = qaTestDataPath + File.separator + "activitystore";
        log.info("Activity Store test data directory is " + activityStoreTestDataPath);
        redshiftJdbcTemplate = redshiftPartitionService.getBatchUserJdbcTemplate(null);
        testBed.attachProtectedProxy(plsCDLSoftDeleteProxy);
    }

    @Test(groups = { "qa-activitystore-end2end" }, description = "Import all data for activity store tests")
    public void testImportActivityStoreData() throws TimeoutException {
        log.info("Cleanup activity stream data...");
        String email = MultiTenantContext.getEmailAddress();
        cdlProxy.cleanupAllByAction(mainCustomerSpace, BusinessEntity.ActivityStream, email);

        log.info("Starting file import...");

        log.info("Importing account data...");
        String defaultAccountFilePath = activityStoreTestDataPath + File.separator + DEFAULT_ACCOUNT_FILE_NAME;
        testFileImportService.doDefaultTemplateOneOffImport(defaultAccountFilePath, EntityType.Accounts);
        String otherAccountFilePath = activityStoreTestDataPath + File.separator + OTHER_ACCOUNT_FILE_NAME;
        testFileImportService.doOneOffImport(otherAccountFilePath, OTHER_SYSTEM_NAME, EntityType.Accounts);

        log.info("Importing contact data...");
        String defaultContactFilePath = activityStoreTestDataPath + File.separator + DEFAULT_CONTACT_FILE_NAME;
        testFileImportService.doDefaultTemplateOneOffImport(defaultContactFilePath, EntityType.Contacts);
        testFileImportService.doOneOffImport(defaultContactFilePath, OTHER_SYSTEM_NAME, EntityType.Contacts);

        log.info("Importing DnbIntent data...");
        String intentFilePath = activityStoreTestDataPath + File.separator + INTENT_FILE_NAME;
        File intentFile = new File(intentFilePath);
        String bucketName = getIntentS3BucketName();
        S3Utilities.uploadFileToS3(bucketName, intentFile);

        log.info("Importing Webvisit data...");
        String webvisitFilePath = activityStoreTestDataPath + File.separator + WEBVISIT_FILE_NAME;
        testFileImportService.doOneOffImport(webvisitFilePath, WEBVISIT_SYSTEM, EntityType.WebVisit);

        log.info("Importing Opportunity data...");
        String opportunityFilePath = activityStoreTestDataPath + File.separator + OPPORTUNITY_FILE_NAME;
        testFileImportService.doDefaultTemplateOneOffImport(opportunityFilePath, EntityType.Opportunity);
        testFileImportService.doOneOffImport(opportunityFilePath, OTHER_SYSTEM_NAME, EntityType.Opportunity);

        log.info("Importing Marketing data...");
        String marketingFilePath = activityStoreTestDataPath + File.separator + MARKETING_FILE_NAME;
        testFileImportService.doDefaultTemplateOneOffImport(marketingFilePath, EntityType.MarketingActivity);
        testFileImportService.doOneOffImport(marketingFilePath, OTHER_SYSTEM_NAME, EntityType.MarketingActivity);
        // wait all file import actions are done
        log.info("Waiting all file import actions are done...");
        testJobService.waitForProcessAnalyzeAllActionsDone(DEFAULT_WAIT_PA_READY_IN_MINUTES);

        // run PA
        log.info("Starting PA for file import...");
        ProcessAnalyzeRequest processAnalyzeRequest = new ProcessAnalyzeRequest();
        processAnalyzeRequest.setRebuildEntities(ImmutableSet.of(BusinessEntity.ActivityStream));
        testJobService.processAnalyze(mainTestTenant, true, processAnalyzeRequest);
    }

    @Test(groups = {
            "qa-activitystore-end2end" }, description = "Account correctness check for activity store tests", dependsOnMethods = {
                    "testImportActivityStoreData" })
    public void testAccountImportCorrectness() throws TimeoutException {
        allAccounts = queryAccountRecords();
        Assert.assertEquals(allAccounts.size(), 58, "Account number is incorrect!!");
    }

    @Test(groups = {
            "qa-activitystore-end2end" }, description = "Contact correctness check for activity store tests", dependsOnMethods = {
                    "testImportActivityStoreData" })
    public void testContactImportCorrectness() throws TimeoutException {
        allContacts = queryContactRecords();
        Assert.assertEquals(allContacts.size(), 132, "Contact number is incorrect!!");
    }

    @Test(groups = {
            "qa-activitystore-end2end" }, description = "CustomIntent correctness check for activity store tests", dependsOnMethods = {
                    "testImportActivityStoreData" })
    public void testCustomIntentImportCorrectness() throws TimeoutException {
        JSONArray actualResult = queryCustomIntentRecords();
        Util.assertResult(activityStoreTestDataPath + "/importresults/customintent.json", actualResult);
    }

    @Test(groups = {
            "qa-activitystore-end2end" }, description = "Opportunity correctness check for activity store tests", dependsOnMethods = {
                    "testImportActivityStoreData" })
    public void testOpportunityImportCorrectness() throws TimeoutException {
        JSONArray actualResult = queryOpportunityRecords();
        Util.assertResult(activityStoreTestDataPath + "/importresults/opportunity.json", actualResult);
    }

    @Test(groups = {
            "qa-activitystore-end2end" }, description = "WebVisit correctness check for activity store tests", dependsOnMethods = {
                    "testImportActivityStoreData" })
    public void testWebVisitImportCorrectness() throws TimeoutException {
        JSONArray actualResult = queryWebVisitRecords();
        Util.assertResult(activityStoreTestDataPath + "/importresults/webvisit.json", actualResult);
    }

    @Test(groups = {
            "qa-activitystore-end2end" }, description = "Marketing Activity correctness check for activity store tests", dependsOnMethods = {
                    "testImportActivityStoreData" })
    public void testMarketingImportCorrectness() throws TimeoutException {
        JSONArray actualResult = queryAccountMarketingRecords();
        Util.assertResult(activityStoreTestDataPath + "/importresults/accountmarketing.json", actualResult);
        actualResult = queryContactMarketingRecords();
        Util.assertResult(activityStoreTestDataPath + "/importresults/contactmarketing.json", actualResult);
    }

    @Test(groups = {
            "qa-activitystore-end2end" }, description = "Activity store soft delete tests", dependsOnMethods = {
                    "testAccountImportCorrectness", "testContactImportCorrectness", "testCustomIntentImportCorrectness",
                    "testOpportunityImportCorrectness", "testWebVisitImportCorrectness",
                    "testMarketingImportCorrectness" })
    public void testActivityStoreDelete() throws TimeoutException {
        registerDeleteData(activityStoreTestDataPath + "/SoftDeleteTestScenarios.csv", false);
        // wait all file import actions are done
        log.info("Waiting all soft delete actions are done...");
        testJobService.waitForProcessAnalyzeAllActionsDone(DEFAULT_WAIT_PA_READY_IN_MINUTES);

        log.info("Starting PA for soft delete...");
        testJobService.processAnalyzeRunNow(mainTestTenant);
    }

    @Test(groups = {
            "qa-activitystore-end2end" }, description = "Account correctness check after soft delete tests", dependsOnMethods = {
                    "testActivityStoreDelete" })
    public void testAccountSoftDeleteCorrectness() throws TimeoutException {
        List<Map<String, Object>> allAccounts = queryAccountRecords();
        Assert.assertEquals(allAccounts.size(), 54, "Account number is incorrect!!");
    }

    @Test(groups = {
            "qa-activitystore-end2end" }, description = "Contact correctness check for soft delete tests", dependsOnMethods = {
                    "testActivityStoreDelete" })
    public void testContactSoftDeleteCorrectness() throws TimeoutException {
        List<Map<String, Object>> allContacts = queryContactRecords();
        Assert.assertEquals(allContacts.size(), 104, "Contact number is incorrect!!");
    }

    @Test(groups = {
            "qa-activitystore-end2end" }, description = "CustomIntent correctness check for soft delete tests", dependsOnMethods = {
                    "testActivityStoreDelete" })
    public void testCustomIntentSoftDeleteCorrectness() throws TimeoutException {
        JSONArray actualResult = queryCustomIntentRecords();
        Util.assertResult(activityStoreTestDataPath + "/softdeleteresults/customintent.json", actualResult);
    }

    @Test(groups = {
            "qa-activitystore-end2end" }, description = "Opportunity correctness check for soft delete tests", dependsOnMethods = {
                    "testActivityStoreDelete" })
    public void testOpportunitySoftDeleteCorrectness() throws TimeoutException {
        JSONArray actualResult = queryOpportunityRecords();
        Util.assertResult(activityStoreTestDataPath + "/softdeleteresults/opportunity.json", actualResult);
    }

    @Test(groups = {
            "qa-activitystore-end2end" }, description = "WebVisit correctness check for soft delete tests", dependsOnMethods = {
                    "testActivityStoreDelete" })
    public void testWebVisitSoftDeleteCorrectness() throws TimeoutException {
        JSONArray actualResult = queryWebVisitRecords();
        Util.assertResult(activityStoreTestDataPath + "/softdeleteresults/webvisit.json", actualResult);
    }

    @Test(groups = {
            "qa-activitystore-end2end" }, description = "Marketing Activity correctness check for soft delete tests", dependsOnMethods = {
                    "testActivityStoreDelete" })
    public void testMarketingSoftDeleteCorrectness() throws TimeoutException {
        JSONArray actualResult = queryAccountMarketingRecords();
        Util.assertResult(activityStoreTestDataPath + "/softdeleteresults/accountmarketing.json", actualResult);
        actualResult = queryContactMarketingRecords();
        Util.assertResult(activityStoreTestDataPath + "/softdeleteresults/contactmarketing.json", actualResult);
    }

    @Test(groups = { "qa-activitystore-end2end" }, description = "Activity store rematch tests", dependsOnMethods = {
            "testAccountSoftDeleteCorrectness", "testContactSoftDeleteCorrectness",
            "testCustomIntentSoftDeleteCorrectness", "testOpportunitySoftDeleteCorrectness",
            "testWebVisitSoftDeleteCorrectness", "testMarketingSoftDeleteCorrectness" })
    public void testActivityStoreRematch() throws TimeoutException {
        log.info("Importing deleted account data...");
        String accountReimportFilePath = activityStoreTestDataPath + File.separator + ACCOUNT_REIMPORT_FILE_NAME;
        testFileImportService.doDefaultTemplateOneOffImport(accountReimportFilePath, EntityType.Accounts);
        testFileImportService.doOneOffImport(accountReimportFilePath, OTHER_SYSTEM_NAME, EntityType.Accounts);

        log.info("Importing deleted contact data...");
        String contactReimportFilePath = activityStoreTestDataPath + File.separator + CONTACT_REIMPORT_FILE_NAME;
        testFileImportService.doDefaultTemplateOneOffImport(contactReimportFilePath, EntityType.Contacts);
        // wait all file import actions are done
        log.info("Waiting all file import actions are done...");
        testJobService.waitForProcessAnalyzeAllActionsDone(DEFAULT_WAIT_PA_READY_IN_MINUTES);

        // run PA
        log.info("Starting PA for file import...");
        ProcessAnalyzeRequest processAnalyzeRequest = new ProcessAnalyzeRequest();
        processAnalyzeRequest.setRebuildEntities(Collections.singleton(BusinessEntity.ActivityStream));
        testJobService.processAnalyze(mainTestTenant, true, processAnalyzeRequest);
    }

    @Test(groups = {
            "qa-activitystore-end2end" }, description = "Account correctness check after rematch tests", dependsOnMethods = {
                    "testActivityStoreRematch" })
    public void testAccountRematchCorrectness() throws TimeoutException {
        List<Map<String, Object>> allAccounts = queryAccountRecords();
        Assert.assertEquals(allAccounts.size(), 58, "Account number is incorrect!!");
    }

    @Test(groups = {
            "qa-activitystore-end2end" }, description = "Contact correctness check for rematch tests", dependsOnMethods = {
                    "testActivityStoreRematch" })
    public void testContactRematchCorrectness() throws TimeoutException {
        List<Map<String, Object>> allContacts = queryContactRecords();
        Assert.assertEquals(allContacts.size(), 132, "Contact number is incorrect!!");
    }

    @Test(groups = {
            "qa-activitystore-end2end" }, description = "CustomIntent correctness check for rematch tests", dependsOnMethods = {
                    "testActivityStoreRematch" })
    public void testCustomIntentRematchCorrectness() throws TimeoutException {
        JSONArray actualResult = queryCustomIntentRecords();
        Util.assertResult(activityStoreTestDataPath + "/rematchresults/customintent.json", actualResult);
    }

    @Test(groups = {
            "qa-activitystore-end2end" }, description = "Opportunity correctness check for rematch tests", dependsOnMethods = {
                    "testActivityStoreRematch" })
    public void testOpportunityRematchCorrectness() throws TimeoutException {
        JSONArray actualResult = queryOpportunityRecords();
        Util.assertResult(activityStoreTestDataPath + "/rematchresults/opportunity.json", actualResult);
    }

    @Test(groups = {
            "qa-activitystore-end2end" }, description = "WebVisit correctness check for rematch tests", dependsOnMethods = {
                    "testActivityStoreRematch" })
    public void testWebVisitRematchCorrectness() throws TimeoutException {
        JSONArray actualResult = queryWebVisitRecords();
        Util.assertResult(activityStoreTestDataPath + "/rematchresults/webvisit.json", actualResult);
    }

    @Test(groups = {
            "qa-activitystore-end2end" }, description = "Marketing Activity correctness check for rematch tests", dependsOnMethods = {
                    "testActivityStoreRematch" })
    public void testMarketingRematchCorrectness() throws TimeoutException {
        JSONArray actualResult = queryAccountMarketingRecords();
        Util.assertResult(activityStoreTestDataPath + "/rematchresults/accountmarketing.json", actualResult);
        actualResult = queryContactMarketingRecords();
        Util.assertResult(activityStoreTestDataPath + "/rematchresults/contactmarketing.json", actualResult);
    }

    @Test(groups = {
            "qa-activitystore-end2end" }, description = "Test hard delete for accounts which have activity store data", dependsOnMethods = {
                    "testAccountRematchCorrectness", "testContactRematchCorrectness",
                    "testCustomIntentRematchCorrectness", "testOpportunityRematchCorrectness",
                    "testWebVisitRematchCorrectness", "testMarketingRematchCorrectness" }, enabled = false)
    public void testAccountHardDelete() throws TimeoutException {
        registerDeleteData(activityStoreTestDataPath + "/HardDeleteTestScenarios.csv", true);
        // wait all file import actions are done
        log.info("Waiting all hard delete actions are done...");
        testJobService.waitForProcessAnalyzeAllActionsDone(DEFAULT_WAIT_PA_READY_IN_MINUTES);

        log.info("Starting PA for hard delete...");
        ProcessAnalyzeRequest processAnalyzeRequest = new ProcessAnalyzeRequest();
        processAnalyzeRequest.setFullRematch(true);
        processAnalyzeRequest.setRebuildEntities(Collections.singleton(BusinessEntity.ActivityStream));
        testJobService.processAnalyze(mainTestTenant, true, processAnalyzeRequest);
    }

    @Test(groups = {
            "qa-activitystore-end2end" }, description = "Account correctness check after account hard delete tests", dependsOnMethods = {
                    "testAccountHardDelete" }, enabled = false)
    public void testAccountHardDeleteCorrectness() throws TimeoutException {
        List<Map<String, Object>> allAccounts = queryAccountRecords();
        Assert.assertEquals(allAccounts.size(), 54, "Account number is incorrect!!");
    }

    @Test(groups = {
            "qa-activitystore-end2end" }, description = "Contact correctness check for account hard delete tests", dependsOnMethods = {
                    "testAccountHardDelete" }, enabled = false)
    public void testContactHardDeleteCorrectness() throws TimeoutException {
        List<Map<String, Object>> allContacts = queryContactRecords();
        Assert.assertEquals(allContacts.size(), 120, "Contact number is incorrect!!");
    }

    @Test(groups = {
            "qa-activitystore-end2end" }, description = "CustomIntent correctness check for account hard delete tests", dependsOnMethods = {
                    "testAccountHardDelete" }, enabled = false)
    public void testCustomIntentHardDeleteCorrectness() throws TimeoutException {
        JSONArray actualResult = queryCustomIntentRecords();
        Util.assertResult(activityStoreTestDataPath + "/harddeleteresults/customintent.json", actualResult);
    }

    @Test(groups = {
            "qa-activitystore-end2end" }, description = "Opportunity correctness check for account hard delete tests", dependsOnMethods = {
                    "testAccountHardDelete" }, enabled = false)
    public void testOpportunityHardDeleteCorrectness() throws TimeoutException {
        JSONArray actualResult = queryOpportunityRecords();
        Util.assertResult(activityStoreTestDataPath + "/harddeleteresults/opportunity.json", actualResult);
    }

    @Test(groups = {
            "qa-activitystore-end2end" }, description = "WebVisit correctness check for account hard delete tests", dependsOnMethods = {
                    "testAccountHardDelete" }, enabled = false)
    public void testWebVisitHardDeleteCorrectness() throws TimeoutException {
        JSONArray actualResult = queryWebVisitRecords();
        Util.assertResult(activityStoreTestDataPath + "/harddeleteresults/webvisit.json", actualResult);
    }

    @Test(groups = {
            "qa-activitystore-end2end" }, description = "Marketing Activity correctness check for account hard delete tests", dependsOnMethods = {
                    "testAccountHardDelete" }, enabled = false)
    public void testMarketingHardDeleteCorrectness() throws TimeoutException {
        JSONArray actualResult = queryAccountMarketingRecords();
        Util.assertResult(activityStoreTestDataPath + "/harddeleteresults/accountmarketing.json", actualResult);
        actualResult = queryContactMarketingRecords();
        Util.assertResult(activityStoreTestDataPath + "/harddeleteresults/contactmarketing.json", actualResult);
    }

    private String getIntentS3BucketName() {
        DropBoxSummary dropBoxSummary = dropBoxProxy.getDropBox(mainTestTenant.getName());
        // Get intent bucket name
        String bucketName = String.format(INTENT_TEMPLATE_PATH, dropBoxSummary.getBucket(),
                dropBoxSummary.getDropBox());
        log.info("Intent bucket name is " + bucketName);
        return bucketName;
    }

    private List<Map<String, Object>> getRecords(Table table, String columns) {
        Assert.assertNotNull(table);
        List<Map<String, Object>> result = redshiftJdbcTemplate
                .queryForList(String.format("SELECT %s FROM %s", columns == null ? "*" : columns, table.getName()));
        return result;
    }

    private List<Map<String, Object>> queryAccountRecords() {
        Table table = dataCollectionProxy.getTable(mainTestTenant.getId(), TableRoleInCollection.BucketedAccount);
        List<Attribute> tbAttributes = table.getAttributes();
        String defaultSysAccountidName = getSystemAccountidColumnName(tbAttributes, "DefaultSystem");
        String otherSysAccountidName = getSystemAccountidColumnName(tbAttributes, OTHER_SYSTEM_NAME);

        List<Map<String, Object>> entityRecords = getRecords(table,
                String.format("accountid, companyname, %s as defaultsystem_accountid, %s as othersystem_accountid",
                        defaultSysAccountidName, otherSysAccountidName));
        return entityRecords;
    }

    private String getSystemAccountidColumnName(List<Attribute> tbAttributes, String systemName) {
        Attribute sysAccountidAtt = tbAttributes.stream().filter(attribute -> attribute.getName().contains(systemName))
                .findFirst().orElse(null);
        return sysAccountidAtt.getName().toLowerCase();
    }

    private List<Map<String, Object>> queryContactRecords() {
        Table table = dataCollectionProxy.getTable(mainTestTenant.getId(), TableRoleInCollection.SortedContact);
        List<Attribute> tbAttributes = table.getAttributes();
        String defaultSysContactidName = getSystemContactidColumnName(tbAttributes, "DefaultSystem");
        String otherSysContactidName = getSystemContactidColumnName(tbAttributes, OTHER_SYSTEM_NAME);

        List<Map<String, Object>> entityRecords = getRecords(table,
                String.format("contactid, %s as defaultsystem_contactid, %s as othersystem_contactid",
                        defaultSysContactidName, otherSysContactidName));
        return entityRecords;
    }

    private String getSystemContactidColumnName(List<Attribute> tbAttributes, String systemName) {
        Attribute sysContactidAtt = tbAttributes.stream().filter(
                attribute -> attribute.getName().contains(systemName) && attribute.getName().contains("ContactId"))
                .findFirst().orElse(null);
        return sysContactidAtt.getName().toLowerCase();
    }

    private String getLatticeAccountid(String defaultSystemAccountid) {
        return getAccountAttributeValue("defaultsystem_accountid", defaultSystemAccountid, "accountid");
    }

    private String getAccountAttributeValue(String byAttributeName, String byAttributeValue, String queryAttribute) {
        if (allAccounts == null) {
            allAccounts = queryAccountRecords();
        }
        Map<String, Object> accountMap = allAccounts.stream()
                .filter(account -> byAttributeValue.equals(account.get(byAttributeName))).findFirst().orElse(null);
        return accountMap.get(queryAttribute).toString();
    }

    private String getLatticeContactid(String defaultSystemContactid) {
        return getContactAttributeValue("defaultsystem_contactid", defaultSystemContactid, "contactid");
    }

    private String getContactAttributeValue(String byAttributeName, String byAttributeValue, String queryAttribute) {
        if (allContacts == null) {
            allContacts = queryContactRecords();
        }
        Map<String, Object> contactMap = allContacts.stream()
                .filter(contact -> byAttributeValue.equals(contact.get(byAttributeName))).findFirst().orElse(null);
        return contactMap.get(queryAttribute) != null ? contactMap.get(queryAttribute).toString() : null;
    }

    private JSONArray queryCustomIntentRecords() {
        return buildAccountLevelResult(TableRoleInCollection.CustomIntentProfile, false);
    }

    private JSONArray queryOpportunityRecords() {
        AttrConfigRequest attrConfigRequest = cdlAttrConfigProxy.getAttrConfigByCategory(mainTestTenant.getId(),
                Category.OPPORTUNITY_PROFILE.getName());
        List<AttrConfig> attrConfigList = attrConfigRequest.getAttrConfigs();
        Table table = dataCollectionProxy.getTable(mainTestTenant.getId(), TableRoleInCollection.OpportunityProfile);
        Attribute accountidAtt = table.getAttribute(InterfaceName.AccountId.name());
        List<Map<String, Object>> entityRecords = getRecords(table, null);
        JSONArray resArray = new JSONArray();
        for (Map<String, Object> record : entityRecords) {
            JSONObject obj = new JSONObject();
            JSONObject subObj = new JSONObject();
            for (AttrConfig attribute : attrConfigList) {
                subObj.put(attribute.getAttrProps().get("DisplayName").getSystemValue().toString(),
                        record.get(attribute.getAttrName()));
            }
            String accountName = getAccountAttributeValue("accountid", record.get(accountidAtt.getName()).toString(),
                    "companyname");
            obj.put(accountName, subObj);
            resArray.put(obj);
        }
        log.debug(resArray.toString(4));
        return resArray;
    }

    private JSONArray queryWebVisitRecords() {
        return buildAccountLevelResult(TableRoleInCollection.WebVisitProfile, true);
    }

    private JSONArray queryAccountMarketingRecords() {
        return buildAccountLevelResult(TableRoleInCollection.AccountMarketingActivityProfile, true);
    }

    private JSONArray buildAccountLevelResult(TableRoleInCollection tableRole, boolean needSubcategory) {
        Table table = dataCollectionProxy.getTable(mainTestTenant.getId(), tableRole);
        List<Attribute> tbAttributes = table.getAttributes();
        Attribute accountidAtt = table.getAttribute(InterfaceName.AccountId.name());
        tbAttributes.remove(accountidAtt);
        List<Map<String, Object>> entityRecords = getRecords(table, null);
        JSONArray resArray = new JSONArray();
        for (Map<String, Object> record : entityRecords) {
            JSONObject obj = new JSONObject();
            JSONObject subObj = new JSONObject();
            for (Attribute attribute : tbAttributes) {
                if (needSubcategory) {
                    subObj.put(attribute.getPropertyValue("Subcategory") + " - " + attribute.getDisplayName(),
                            record.get(attribute.getName()));
                } else {
                    subObj.put(attribute.getDisplayName(), record.get(attribute.getName()));
                }
            }
            String accountName = getAccountAttributeValue("accountid", record.get(accountidAtt.getName()).toString(),
                    "companyname");
            obj.put(accountName, subObj);
            resArray.put(obj);
        }
        log.debug(resArray.toString(4));
        return resArray;
    }

    private JSONArray queryContactMarketingRecords() {
        Table table = dataCollectionProxy.getTable(mainTestTenant.getId(),
                TableRoleInCollection.ContactMarketingActivityProfile);
        List<Attribute> tbAttributes = table.getAttributes();
        Attribute contactidAtt = table.getAttribute(InterfaceName.ContactId.name());
        tbAttributes.remove(contactidAtt);
        List<Map<String, Object>> entityRecords = getRecords(table, null);
        JSONArray resArray = new JSONArray();
        for (Map<String, Object> record : entityRecords) {
            JSONObject obj = new JSONObject();
            JSONObject subObj = new JSONObject();
            for (Attribute attribute : tbAttributes) {
                subObj.put(attribute.getPropertyValue("Subcategory") + " - " + attribute.getDisplayName(),
                        record.get(attribute.getName()));
            }
            String defaultSysContactid = getContactAttributeValue("contactid",
                    record.get(contactidAtt.getName()).toString(), "defaultsystem_contactid");
            String otherSysContactid = getContactAttributeValue("contactid",
                    record.get(contactidAtt.getName()).toString(), "othersystem_contactid");
            String contactName = defaultSysContactid == null ? "OtherSystem_" + otherSysContactid
                    : "DefaultSystem_" + defaultSysContactid;
            obj.put(contactName, subObj);
            resArray.put(obj);
        }
        log.debug(resArray.toString());
        return resArray;
    }

    private void registerDeleteData(String scenarioFile, boolean hardDelete) {
        List<DeleteRequest> allRequests = new ArrayList<>();
        Util.parseFile(scenarioFile, csvParser -> {
            for (CSVRecord record : csvParser) {
                String testCase = record.get("Name");
                logger.info(String.format("\n\r\t%sDeleteTestCase%s, is for %s.", hardDelete ? "Hard" : "Soft",
                        record.get("No"), testCase));
                DeleteRequest deleteRequest = new DeleteRequest();
                deleteRequest
                        .setDeleteEntityType(EntityType.fromDisplayNameToEntityType(record.get("DeleteEntityType")));
                String fromDate = record.get("FromDate");
                if (!StringUtils.isEmpty(fromDate))
                    deleteRequest.setFromDate(fromDate);
                String toDate = record.get("ToDate");
                if (!StringUtils.isEmpty(toDate))
                    deleteRequest.setToDate(toDate);
                String idEntity = record.get("IdEntity");
                if (!StringUtils.isEmpty(idEntity)) {
                    deleteRequest.setIdEntity(BusinessEntity.getByName(idEntity));
                }
                String idSystem = record.get("IdSystem");
                if (!StringUtils.isEmpty(idSystem)) {
                    deleteRequest.setIdSystem(idSystem);
                }
                if (hardDelete)
                    deleteRequest.setHardDelete(true);
                String idsStr = record.get("IDs");
                if (!StringUtils.isEmpty(idsStr)) {
                    String[] ids = idsStr.split(",");
                    boolean needTransform = Boolean.parseBoolean(record.get("DoTransform"));
                    StringBuilder sb = new StringBuilder();
                    SchemaInterpretation schemaInterpretation = null;
                    if (deleteRequest.getIdEntity() == BusinessEntity.Account) {
                        schemaInterpretation = SchemaInterpretation.DeleteByAccountTemplate;
                        sb.append("AccountId");
                    } else {
                        schemaInterpretation = SchemaInterpretation.DeleteByContactTemplate;
                        sb.append("ContactId");
                    }
                    sb.append('\n');
                    for (String id : ids) {
                        if (needTransform) {
                            if (deleteRequest.getIdEntity() == BusinessEntity.Account)
                                sb.append(getLatticeAccountid(id));
                            else
                                sb.append(getLatticeContactid(id));
                        } else {
                            sb.append(id);
                        }
                        sb.append('\n');
                    }
                    String fileName = testCase + ".csv";
                    log.info("\n" + sb.toString());
                    Resource source = new ByteArrayResource(sb.toString().getBytes()) {
                        @Override
                        public String getFilename() {
                            return fileName;
                        }
                    };
                    SourceFile sourceFile = uploadDeleteCSV(fileName, schemaInterpretation,
                            CleanupOperationType.BYUPLOAD_ID, source);
                    deleteRequest.setFilename(sourceFile.getName());
                }
                allRequests.add(deleteRequest);
            }
            return null;
        });
        for (DeleteRequest deleteRequest : allRequests) {
            plsCDLSoftDeleteProxy.delete(deleteRequest);
        }
    }

}
