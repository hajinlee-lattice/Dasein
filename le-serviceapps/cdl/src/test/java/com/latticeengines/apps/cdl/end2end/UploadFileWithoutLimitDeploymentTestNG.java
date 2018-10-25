package com.latticeengines.apps.cdl.end2end;

import java.io.IOException;
import java.util.Collections;

import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class UploadFileWithoutLimitDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    private boolean outsizeFlag = true;
    @Value("${cdl.largeimport.account.filename:}")
    private String account_csv;
    @Value("${cdl.largeimport.contact.filename:}")
    private String contact_csv;
    @Value("${cdl.largeimport.transaction.filename:}")
    private String transaction_csv;

    @BeforeClass(groups = { "manual" })
    public void setup() throws Exception {
        setupEnd2EndTestEnvironment();
        testBed.excludeTestTenantsForCleanup(Collections.singletonList(mainTestTenant));
    }

    @Test(groups = "manual")
    public void runTest() throws Exception {
        importData(BusinessEntity.Account, outsizeFlag);
        clearHdfs();
        importData(BusinessEntity.Contact, outsizeFlag);
        clearHdfs();
        importData(BusinessEntity.Transaction, outsizeFlag);
    }

    private void importData(BusinessEntity importingEntity, boolean outsizeFlag) {
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.Initialized.getName());

        if (importingEntity.equals(BusinessEntity.Account)) {
            importData(BusinessEntity.Account, account_csv, "Account", false, outsizeFlag);
        }

        if (importingEntity.equals(BusinessEntity.Contact)) {
            importData(BusinessEntity.Contact, contact_csv, "Contact", true, outsizeFlag);
        }

        if (importingEntity.equals(BusinessEntity.Transaction)) {
            importData(BusinessEntity.Transaction, transaction_csv, "Transaction", true, outsizeFlag);
        }
    }

    private void clearHdfs() throws IOException {
        CustomerSpace customerSpace = CustomerSpace.parse(mainTestTenant.getId());
        String dataFeedDir = String.format("%s/%s/DataFeed1",
                PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), customerSpace).toString(),
                SourceType.FILE.getName());
        HdfsUtils.rmdir(yarnConfiguration, dataFeedDir);
    }
}
