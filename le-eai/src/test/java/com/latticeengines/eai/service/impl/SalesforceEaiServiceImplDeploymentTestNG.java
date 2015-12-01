package com.latticeengines.eai.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertNotNull;

import java.util.Arrays;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.exposed.service.MetadataService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.source.SourceCredentialType;
import com.latticeengines.eai.exposed.service.EaiService;
import com.latticeengines.eai.functionalframework.EaiFunctionalTestNGBase;
import com.latticeengines.eai.service.DataExtractionService;
import com.latticeengines.eai.service.EaiMetadataService;
import com.latticeengines.remote.exposed.service.CrmCredentialZKService;

public class SalesforceEaiServiceImplDeploymentTestNG extends EaiFunctionalTestNGBase {

    @Autowired
    private EaiService eaiService;

    @Autowired
    private MetadataService metadataService;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private CrmCredentialZKService crmCredentialZKService;

    @Autowired
    private DataExtractionService dataExtractionService;

    @Autowired
    private EaiMetadataService eaiMetadataService;

    @Value("${eai.test.salesforce.username}")
    private String salesforceUserName;

    @Value("${eai.test.salesforce.password}")
    private String salesforcePasswd;

    @Value("${eai.test.salesforce.securitytoken}")
    private String salesforceSecurityToken;

    @Value("${eai.salesforce.production.loginurl}")
    private String productionLoginUrl;

    private List<String> tableNameList = Arrays.<String> asList(new String[] { "Account", "Contact", "Lead",
            "Opportunity", "OpportunityContactRole" });

    private String customer = "SFDC-Eai-Customer";

    private String targetPath;

    private String customerSpace = CustomerSpace.parse(customer).toString();

    private Tenant tenant;

    private List<Table> tables;

    @BeforeClass(groups = "deployment")
    private void setup() throws Exception {
        targetPath = dataExtractionService.createTargetPath(customer);
        HdfsUtils.rmdir(yarnConfiguration, targetPath);

        initZK(customer);
        crmCredentialZKService.removeCredentials("sfdc", customer, true);
        CrmCredential crmCredential = new CrmCredential();
        crmCredential.setUserName(salesforceUserName);
        crmCredential.setPassword(salesforcePasswd);
        crmCredential.setSecurityToken(salesforceSecurityToken);
        crmCredential.setUrl(productionLoginUrl);
        crmCredentialZKService.writeToZooKeeper("sfdc", customer, true, crmCredential, true);

        crmCredential.setPassword(salesforcePasswd);
        crmCredentialZKService.writeToZooKeeper("sfdc", customer, false, crmCredential, true);

        tenant = createTenant(customerSpace);
        try {
            tenantService.discardTenant(tenant);
        } catch (Exception e) {
        }
        tenantService.registerTenant(tenant);

        tables = getSalesforceTables(tableNameList);
        System.out.println(tables);
        eaiMetadataService.createImportTables(customerSpace, tables);
    }

    @AfterClass(groups = "deployment")
    private void cleanUp() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, PathBuilder.buildContractPath(CamilleEnvironment.getPodId(), customer)
                .toString());
        Camille camille = CamilleEnvironment.getCamille();
        camille.delete(PathBuilder.buildContractPath(CamilleEnvironment.getPodId(), customer));
        tenantService.discardTenant(tenant);
    }

    @Test(groups = { "deployment" }, enabled = true)
    public void extractAndImport() throws Exception {
        ImportConfiguration importConfig = createSalesforceImportConfig(customer);
        targetPath += "/" + importConfig.getSourceConfigurations().get(0).getSourceType().getName();
        ApplicationId appId = eaiService.extractAndImport(importConfig);
        assertNotNull(appId);
        FinalApplicationStatus status = platformTestBase.waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);

        checkDataExists(targetPath, tableNameList, 1);
        List<Table> tablesBeforeExtract = tables;
        List<Table> tablesAfterExtract = eaiMetadataService.getTables(customerSpace);
        checkLastModifiedTimestampChanged(true, tablesBeforeExtract, tablesAfterExtract);
        checkExtract(tablesAfterExtract);

        HdfsUtils.rmdir(yarnConfiguration, targetPath);
        importConfig.getSourceConfigurations().get(0).setSourceCredentialType(SourceCredentialType.SANDBOX);
        appId = eaiService.extractAndImport(importConfig);
        assertNotNull(appId);
        status = platformTestBase.waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);

        checkDataExists(targetPath, tableNameList, 1);
        tablesBeforeExtract = tablesAfterExtract;
        tablesAfterExtract = eaiMetadataService.getTables(customerSpace);
        checkLastModifiedTimestampChanged(false, tablesBeforeExtract, tablesAfterExtract);
        checkLastModifiedTimestampInRecords(tablesBeforeExtract);
        checkExtract(tablesAfterExtract);

    }

    private void checkExtract(List<Table> tablesAfterExtract) {
        for (Table table : tablesAfterExtract) {
            for (Extract extract : table.getExtracts()) {
                assertTrue(extract.getProcessedRecords() >= 0);
            }
        }
    }

    private void checkLastModifiedTimestampChanged(boolean changed, List<Table> tablesBeforeExtract,
            List<Table> tablesAfterExtract) {

        for (Table tableAfterExtract : tablesAfterExtract) {
            Long lastModifiedValue = tableAfterExtract.getLastModifiedKey().getLastModifiedTimestamp();
            for (Table tableBeforeExtract : tablesBeforeExtract) {
                if (tableAfterExtract.getName().equals(tableBeforeExtract.getName())) {
                    if (changed) {
                        assertEquals(lastModifiedValue.compareTo(tableBeforeExtract.getLastModifiedKey()
                                .getLastModifiedTimestamp()), 1);
                    } else {
                        assertEquals(lastModifiedValue.compareTo(tableBeforeExtract.getLastModifiedKey()
                                .getLastModifiedTimestamp()), 0);
                    }
                }
            }
        }
    }

    private void checkLastModifiedTimestampInRecords(List<Table> tablesBeforeExtract) throws Exception {
        for (Table table : tablesBeforeExtract) {
            List<String> filesForTable = getFilesFromHdfs(targetPath, table.getName());
            List<GenericRecord> records = AvroUtils.getData(yarnConfiguration, new Path(filesForTable.get(0)));
            log.info("Table: " + table.getName());
            for (GenericRecord record : records) {
                Long lastModifiedDateValueFromRecord = (Long) record.get(table.getLastModifiedKey().getAttributeNames()[0]);
                log.info("value from record: " + lastModifiedDateValueFromRecord);
                Long lastModifiedDateValueFromTable = table.getLastModifiedKey().getLastModifiedTimestamp();
                log.info("value from metadata: " + lastModifiedDateValueFromTable);
                assertEquals(lastModifiedDateValueFromTable.compareTo(lastModifiedDateValueFromRecord), 0);
            }
        }
    }

}
