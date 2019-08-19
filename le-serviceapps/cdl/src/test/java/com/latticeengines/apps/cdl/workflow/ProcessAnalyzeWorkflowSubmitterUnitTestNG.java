package com.latticeengines.apps.cdl.workflow;

import static com.latticeengines.domain.exposed.query.BusinessEntity.Account;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Contact;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;

public class ProcessAnalyzeWorkflowSubmitterUnitTestNG {

    @Test(groups = "unit", dataProvider = "systemIdMaps")
    private void testGetSystemIdMaps(List<S3ImportSystem> systems, String defaultAcctSysId, String defaultContSysId,
            List<String> acctSysIds, List<String> contSysIds) {
        // prepare mocked submitter
        CDLProxy cdlProxy = Mockito.mock(CDLProxy.class);
        when(cdlProxy.getS3ImportSystemList(any(String.class))).thenReturn(systems);
        ProcessAnalyzeWorkflowSubmitter submitter = mockSubmitter(cdlProxy);

        Pair<Map<String, String>, Map<String, List<String>>> res = submitter.getSystemIdMaps("test_tenant", true);
        Assert.assertNotNull(res);
        Assert.assertNotNull(res.getLeft());
        Assert.assertNotNull(res.getRight());

        Map<String, String> defaultIdMap = res.getLeft();
        Map<String, List<String>> systemIdsMap = res.getRight();
        Assert.assertEquals(defaultIdMap.size(), 2, "Default system id map should have size 2 (account + contact)");
        Assert.assertEquals(systemIdsMap.size(), 2, "System ids map should have size 2 (account + contact)");

        String resDefaultAcctId = defaultIdMap.get(Account.name());
        String resDefaultContSysId = defaultIdMap.get(Contact.name());
        // check default system IDs
        Assert.assertEquals(resDefaultAcctId, defaultAcctSysId);
        Assert.assertEquals(resDefaultContSysId, defaultContSysId);

        // check all system ID list
        List<String> resAcctIds = systemIdsMap.get(Account.name());
        List<String> resContIds = systemIdsMap.get(Contact.name());
        Assert.assertEquals(resAcctIds, acctSysIds);
        Assert.assertEquals(resContIds, contSysIds);
    }

    /**
     * @return Input S3ImportSystem list, Expected default AcctSysId, Expected
     *         default ContSysId, Expected list of account IDs, Expected list of
     *         contact IDs
     */
    @DataProvider(name = "systemIdMaps")
    private Object[][] getSystemIdMapsTestData() {
        return new Object[][] {
                // No S3ImportSystems setup
                { null, null, null, emptyList(), emptyList() }, //
                // No system ID mapped to LatticeAccount or LatticeContact
                { asList( //
                        fakeS3ImportSystem("AID1", false, null, null, 0), //
                        fakeS3ImportSystem(null, null, "CID2", false, 1), //
                        fakeS3ImportSystem("AID3", false, "CID3", false, 2), //
                        null // Theoretical not expect to get null
                // S3ImportSystem input
                ), null, null, asList("AID1", "AID3"), asList("CID2", "CID3") }, //
                // Has system mapping to LatticeAccount, no mapping to
                // LatticeContact
                { singletonList( //
                        fakeS3ImportSystem("AID1", true, null, null, 0) //
                ), "AID1", null, singletonList("AID1"), emptyList() }, //
                { singletonList( //
                        fakeS3ImportSystem("AID1", true, "CID1", false, 0) //
                ), "AID1", null, singletonList("AID1"), singletonList("CID1") }, //
                { asList( //
                        fakeS3ImportSystem("AID1", false, null, null, 0), //
                        fakeS3ImportSystem("AID2", true, "CID2", false, 0) //
                ), "AID2", null, asList("AID1", "AID2"), singletonList("CID2") }, //
                // Has system mapping to LatticeContact, no mapping to
                // LatticeAccount
                { singletonList( //
                        fakeS3ImportSystem(null, null, "CID1", true, 0) //
                ), null, "CID1", emptyList(), singletonList("CID1") }, //
                { singletonList( //
                        fakeS3ImportSystem("AID1", false, "CID1", true, 0) //
                ), null, "CID1", singletonList("AID1"), singletonList("CID1") }, //
                { asList( //
                        fakeS3ImportSystem(null, null, "CID1", false, 0), //
                        fakeS3ImportSystem("AID2", false, "CID2", true, 1) //
                ), null, "CID2", singletonList("AID2"), asList("CID1", "CID2") }, //
                // Has system mapping to LatticeAccount and LatticeContact
                // (might not be same system)
                { asList( //
                        fakeS3ImportSystem("AID1", true, null, null, 0), //
                        fakeS3ImportSystem("AID2", false, "CID2", true, 1) //
                ), "AID1", "CID2", asList("AID1", "AID2"), singletonList("CID2") }, //
                { asList( //
                        fakeS3ImportSystem("AID1", false, "CID1", true, 0), //
                        fakeS3ImportSystem("AID2", true, null, null, 1) //
                ), "AID2", "CID1", asList("AID1", "AID2"), singletonList("CID1") }, //
                { asList( //
                        fakeS3ImportSystem(null, null, "CID1", false, 0), //
                        null, // Theoretical not expect to get null
                        // S3ImportSystem input
                        fakeS3ImportSystem("AID2", true, "CID2", true, 1) //
                ), "AID2", "CID2", singletonList("AID2"), asList("CID1", "CID2") }, //

        };
    }

    private ProcessAnalyzeWorkflowSubmitter mockSubmitter(CDLProxy cdlProxy) {
        return new ProcessAnalyzeWorkflowSubmitter(null, null, null, null, null, null, null, null, cdlProxy);
    }

    private S3ImportSystem fakeS3ImportSystem(String acctSysId, Boolean mapToLAcct, String contSysId,
            Boolean mapToLCont, int priority) {
        S3ImportSystem sys = new S3ImportSystem();
        sys.setAccountSystemId(acctSysId);
        sys.setMapToLatticeAccount(mapToLAcct);
        sys.setContactSystemId(contSysId);
        sys.setMapToLatticeContact(mapToLCont);
        sys.setPriority(priority);
        return sys;
    }
}
