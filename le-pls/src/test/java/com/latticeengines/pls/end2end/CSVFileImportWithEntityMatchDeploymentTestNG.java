package com.latticeengines.pls.end2end;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.workflow.Report;

public class CSVFileImportWithEntityMatchDeploymentTestNG extends CSVFileImportDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(CSVFileImportWithEntityMatchDeploymentTestNG.class);

    private static final String ACCOUNT_SOURCE_FILE = "Account_With_Invalid_Char.csv";

    private static final String CONTACT_SOURCE_FILE = "Contact_Insufficient_Info.csv";

    private static final String PRODUCT_HIERARCHY_SOURCE_FILE = "Product_Without_Family_File.csv";

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
    public void testImport() {
        prepareBaseData(ENTITY_ACCOUNT);
        getDataFeedTask(ENTITY_ACCOUNT);
        prepareBaseData(ENTITY_TRANSACTION);
        getDataFeedTask(ENTITY_TRANSACTION);

        Table accountTemplate = accountDataFeedTask.getImportTemplate();
        Assert.assertNull(accountTemplate.getAttribute(InterfaceName.AccountId));
        Table transactionTemplate = transactionDataFeedTask.getImportTemplate();
        Assert.assertNotNull(transactionTemplate.getAttribute(InterfaceName.CustomerAccountId));
    }

    @Test(groups = "deployment", enabled = false)
    public void testInvalidFile() throws IOException {
        SourceFile accountFile = uploadSourceFile(ACCOUNT_SOURCE_FILE, ENTITY_ACCOUNT);
        String targetPath = String.format("%s/%s/DataFeed1/DataFeed1-Account/Extracts",
                PathBuilder
                        .buildDataTablePath(CamilleEnvironment.getPodId(), CustomerSpace.parse(mainTestTenant.getId()))
                        .toString(),
                SourceType.FILE.getName());
        startCDLImport(accountFile, ENTITY_ACCOUNT);
        // Spaces/Production/Data/Tables/File/DataFeed1/DataFeed1-Account/Extracts/2019-05-10-17-30-32/SourceFile_file_1557480309588_csv/Extracts/2019-05-10-17-30-32
        verifyAvroFileNumber(accountFile, 48, targetPath);
        SourceFile contactFile = uploadSourceFile(CONTACT_SOURCE_FILE, ENTITY_CONTACT);
        String contactPath = String.format("%s/%s/DataFeed1/DataFeed1-Contact/Extracts",
                PathBuilder
                        .buildDataTablePath(CamilleEnvironment.getPodId(), CustomerSpace.parse(mainTestTenant.getId()))
                        .toString(),
                SourceType.FILE.getName());
        startCDLImport(contactFile, ENTITY_CONTACT);
        // verify the contact number not change
        verifyAvroFileNumber(contactFile, 50, contactPath);
        SourceFile productFile = uploadSourceFile(PRODUCT_HIERARCHY_SOURCE_FILE, ENTITY_PRODUCT);
        verifyFailed(productFile, ENTITY_PRODUCT);

        List<?> list = restTemplate.getForObject(getRestAPIHostPort() + "/pls/reports", List.class);
        List<Report> reports = JsonUtils.convertList(list, Report.class);
        Collections.sort(reports, (one, two) -> one.getCreated().compareTo(two.getCreated()));
        Assert.assertEquals(reports.size(), 3);
        Report accountReport = reports.get(0);
        Report contactReport = reports.get(1);
        Report productReport = reports.get(2);
        verifyReport(accountReport, 2L, 2L, 48L);
        verifyReport(contactReport, 0L, 0L, 50L);
        verifyReport(productReport, 0L, 6L, 0L);

    }

}
