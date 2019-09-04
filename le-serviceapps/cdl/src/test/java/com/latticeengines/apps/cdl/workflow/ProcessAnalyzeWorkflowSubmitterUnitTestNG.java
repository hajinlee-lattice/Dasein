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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.S3ImportSystemService;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.query.EntityType;

public class ProcessAnalyzeWorkflowSubmitterUnitTestNG {

    @Test(groups = "unit", dataProvider = "systemIdMaps")
    private void testGetSystemIdMaps(List<S3ImportSystem> systems, String defaultAcctSysId, String defaultContSysId,
                                     List<String> acctSysIds, List<String> contSysIds) {
        // prepare mocked submitter
        S3ImportSystemService s3ImportSystemService = Mockito.mock(S3ImportSystemService.class);
        when(s3ImportSystemService.getAllS3ImportSystem(any(String.class))).thenReturn(systems);
        ProcessAnalyzeWorkflowSubmitter submitter = mockSubmitter(s3ImportSystemService);

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
                        fakeS3ImportSystem("AID1", false, null, null, 0, null, null), //
                        fakeS3ImportSystem(null, null, "CID2", false, 1, null, null), //
                        fakeS3ImportSystem("AID3", false, "CID3", false, 2, null, null), //
                        null // Theoretical not expect to get null
                        // S3ImportSystem input
                ), null, null, asList("AID1", "AID3"), asList("CID2", "CID3") }, //
                // Has system mapping to LatticeAccount, no mapping to
                // LatticeContact
                { singletonList( //
                        fakeS3ImportSystem("AID1", true, null, null, 0, null, null) //
                ), "AID1", null, singletonList("AID1"), emptyList() }, //
                { singletonList( //
                        fakeS3ImportSystem("AID1", true, "CID1", false, 0, null, null) //
                ), "AID1", null, singletonList("AID1"), singletonList("CID1") }, //
                { asList( //
                        fakeS3ImportSystem("AID1", false, null, null, 0, null, null), //
                        fakeS3ImportSystem("AID2", true, "CID2", false, 0, null, null) //
                ), "AID2", null, asList("AID1", "AID2"), singletonList("CID2") }, //
                // Has system mapping to LatticeContact, no mapping to
                // LatticeAccount
                { singletonList( //
                        fakeS3ImportSystem(null, null, "CID1", true, 0, null, null) //
                ), null, "CID1", emptyList(), singletonList("CID1") }, //
                { singletonList( //
                        fakeS3ImportSystem("AID1", false, "CID1", true, 0, null, null) //
                ), null, "CID1", singletonList("AID1"), singletonList("CID1") }, //
                { asList( //
                        fakeS3ImportSystem(null, null, "CID1", false, 0, null, null), //
                        fakeS3ImportSystem("AID2", false, "CID2", true, 1, null, null) //
                ), null, "CID2", singletonList("AID2"), asList("CID1", "CID2") }, //
                // Has system mapping to LatticeAccount and LatticeContact
                // (might not be same system)
                { asList( //
                        fakeS3ImportSystem("AID1", true, null, null, 0, null, null), //
                        fakeS3ImportSystem("AID2", false, "CID2", true, 1, null, null) //
                ), "AID1", "CID2", asList("AID1", "AID2"), singletonList("CID2") }, //
                { asList( //
                        fakeS3ImportSystem("AID1", false, "CID1", true, 0, null, null), //
                        fakeS3ImportSystem("AID2", true, null, null, 1, null, null) //
                ), "AID2", "CID1", asList("AID1", "AID2"), singletonList("CID1") }, //
                { asList( //
                        fakeS3ImportSystem(null, null, "CID1", false, 0, null, null), //
                        null, // Theoretical not expect to get null
                        // S3ImportSystem input
                        fakeS3ImportSystem("AID2", true, "CID2", true, 1, null, null) //
                ), "AID2", "CID2", singletonList("AID2"), asList("CID1", "CID2") },
                { asList( //
                        fakeS3ImportSystem("AID1", false, null, null, 0, "SAID1", null), //
                        fakeS3ImportSystem(null, null, "CID2", false, 1, null, "SCID1"), //
                        fakeS3ImportSystem("AID3", false, "CID3", false, 2, "SAID2", "SCID2"), //
                        null // Theoretical not expect to get null
                        // S3ImportSystem input
                ), null, null, asList("AID1", "SAID1", "AID3", "SAID2"), asList("CID2", "SCID1", "CID3", "SCID2") },//

        };
    }

    private ProcessAnalyzeWorkflowSubmitter mockSubmitter(S3ImportSystemService s3ImportSystemService) {
        return new ProcessAnalyzeWorkflowSubmitter(null, null, null, null, null, null, null, null,
                s3ImportSystemService);
    }

    private S3ImportSystem fakeS3ImportSystem(String acctSysId, Boolean mapToLAcct, String contSysId,
                                              Boolean mapToLCont, int priority, String secondaryAcctSysId, String secondaryContSysId) {
        S3ImportSystem sys = new S3ImportSystem();
        sys.setAccountSystemId(acctSysId);
        sys.setMapToLatticeAccount(mapToLAcct);
        sys.setContactSystemId(contSysId);
        sys.setMapToLatticeContact(mapToLCont);
        sys.setPriority(priority);
        if (StringUtils.isNotEmpty(secondaryAcctSysId)) {
            sys.addSecondaryAccountId(EntityType.Accounts, secondaryAcctSysId);
        }
        if (StringUtils.isNotEmpty(secondaryContSysId)) {
            sys.addSecondaryContactId(EntityType.Leads, secondaryContSysId);
        }
        return sys;
    }
}
