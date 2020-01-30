package com.latticeengines.apps.cdl.end2end;

import java.io.IOException;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;

public class HardDeleteActivityStoreDeploymentTestNG extends DeleteActivityStoreDeploymentTestNG {

    private static final Logger log = LoggerFactory.getLogger(HardDeleteActivityStoreDeploymentTestNG.class);

    @Test(groups = "end2end")
    public void testHardDelete() throws Exception {
        super.test();
        registerDeleteData(true);

//        if (isLocalEnvironment()) {
            // run PA with fake current time
            processAnalyzeSkipPublishToS3(CURRENT_PA_TIME.toEpochMilli(), true);
//        } else {
//            runTestWithRetry(getCandidateFailingSteps(), CURRENT_PA_TIME.toEpochMilli());
//        }
        verifyAfterPA();
    }

    private void verifyAfterPA() throws IOException {
        log.info("Ids that needs to be removed: " + idSets.toString());
        Table table = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.ConsolidatedActivityStream);
        List<GenericRecord> recordsAfterDelete = getStreamRecords(table);
        for (GenericRecord record : recordsAfterDelete) {
            String accountId = record.get(InterfaceName.AccountId.name()).toString();
            Assert.assertFalse(idSets.contains(accountId), "Should not contain id " + accountId);
        }
    }
}
