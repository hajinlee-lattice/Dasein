package com.latticeengines.apps.cdl.end2end;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class UploadFileWithoutLimitDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(UploadFileWithoutLimitDeploymentTestNG.class);

    private boolean outsizeFlag = true;
    @Value("${cdl.largeimport.account.filename:}")
    private String account_csv;
    @Value("${cdl.largeimport.contact.filename:}")
    private String contact_csv;
    @Value("${cdl.largeimport.transaction.filename:}")
    private String transaction_csv;
    @Value("${cdl.largeimport.transaction.filenum:}")
    private String transaction_filenum;
    @Value("${cdl.largeimport.contact.filenum:}")
    private String contact_filenum;

    @BeforeClass(groups = { "deployment.largeFile" })
    public void setup() throws Exception {
        setupEnd2EndTestEnvironment();
        testBed.excludeTestTenantsForCleanup(Collections.singletonList(mainTestTenant));
    }

    @Test(groups = "deployment.largeFile")
    public void runTest() throws Exception {
        importData(BusinessEntity.Account, outsizeFlag);
        importData(BusinessEntity.Contact, outsizeFlag);
        importData(BusinessEntity.Transaction, outsizeFlag);
        long spendTime = System.currentTimeMillis();
        processAnalyze();
        log.info("Execute processAnalyze Execution time: " + (System.currentTimeMillis() - spendTime) / 1000f
                + " second");
    }

    private void importData(BusinessEntity importingEntity, boolean outsizeFlag) {
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.Initialized.getName());

        long spendTime = 0L;
        if (importingEntity.equals(BusinessEntity.Account)) {
            spendTime = System.currentTimeMillis();
            importData(BusinessEntity.Account, account_csv, null, false, outsizeFlag);
            log.info("Import " + account_csv + " Execution time: " + (System.currentTimeMillis() - spendTime) / 1000f
                    + " second");
        }

        if (importingEntity.equals(BusinessEntity.Contact)) {
            List<String> filenames = new ArrayList<String>();
            int num = Integer.parseInt(contact_filenum);
            for (int i = 1; i <= num; i++) {
                filenames.add(contact_csv + i + ".csv.gz");
            }
            spendTime = System.currentTimeMillis();
            importData(BusinessEntity.Contact, filenames, "Contact", true, outsizeFlag);
            log.info("Import Contact file Execution time: " + (System.currentTimeMillis() - spendTime) / 1000f
                    + " second");
        }

        if (importingEntity.equals(BusinessEntity.Transaction)) {
            List<String> filenames = new ArrayList<String>();
            int num = Integer.parseInt(transaction_filenum);
            for (int i = 1; i <= num; i++) {
                filenames.add(transaction_csv + i + ".csv.gz");
            }
            spendTime = System.currentTimeMillis();
            importData(BusinessEntity.Transaction, filenames, "Transaction", true, outsizeFlag);
            log.info("Import Transation file Execution time: "
                    + (System.currentTimeMillis() - spendTime) / 1000f + " second");
        }
    }

}
