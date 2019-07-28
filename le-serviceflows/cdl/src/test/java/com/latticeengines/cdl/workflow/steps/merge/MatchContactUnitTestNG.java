package com.latticeengines.cdl.workflow.steps.merge;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;

public class MatchContactUnitTestNG {

    @BeforeClass(groups = "unit")
    public void init() {
        MockitoAnnotations.initMocks(this);
    }

    @Test(groups = "unit", dataProvider = "S3ImportSystems")
    public void testGetDefaultSystemId(List<S3ImportSystem> systems, String defaultAcctSysId, String defaultContSysId) {
        CDLProxy cdlProxy = Mockito.mock(CDLProxy.class);
        when(cdlProxy.getS3ImportSystemList(any(String.class))).thenReturn(systems);

        MatchAccount matchContact = new MatchAccount();
        ReflectionTestUtils.setField(matchContact, "customerSpace", new CustomerSpace());
        ReflectionTestUtils.setField(matchContact, "cdlProxy", cdlProxy);
        Assert.assertEquals(matchContact.getDefaultSystemId(BusinessEntity.Account), defaultAcctSysId);
        Assert.assertEquals(matchContact.getDefaultSystemId(BusinessEntity.Contact), defaultContSysId);
    }

    /**
     * @return Input S3ImportSystem list, Expected default AcctSysId, Expected
     *         default ContSysId
     */
    @DataProvider(name = "S3ImportSystems")
    private Object[][] getS3ImportSystems() {
        return new Object[][] {
                // No S3ImportSystems setup
                { null, null, null }, //
                // No system ID mapped to LatticeAccount or LatticeContact
                { Arrays.asList( //
                        fakeS3ImportSystem("AID1", false, null, null), //
                        fakeS3ImportSystem(null, null, "CID2", false), //
                        fakeS3ImportSystem("AID3", false, "CID3", false), //
                        null // Theoretical not expect to get null
                             // S3ImportSystem input
                ), null, null }, //
                // Has system mapping to LatticeAccount, no mapping to
                // LatticeContact
                { Arrays.asList( //
                        fakeS3ImportSystem("AID1", true, null, null) //
                ), "AID1", null }, //
                { Arrays.asList( //
                        fakeS3ImportSystem("AID1", true, "CID1", false) //
                ), "AID1", null }, //
                { Arrays.asList( //
                        fakeS3ImportSystem("AID1", false, null, null), //
                        fakeS3ImportSystem("AID2", true, "CID2", false) //
                ), "AID2", null }, //
                // Has system mapping to LatticeContact, no mapping to
                // LatticeAccount
                { Arrays.asList( //
                        fakeS3ImportSystem(null, null, "CID1", true) //
                ), null, "CID1" }, //
                { Arrays.asList( //
                        fakeS3ImportSystem("AID1", false, "CID1", true) //
                ), null, "CID1" }, //
                { Arrays.asList( //
                        fakeS3ImportSystem(null, null, "CID1", false), //
                        fakeS3ImportSystem("AID2", false, "CID2", true) //
                ), null, "CID2" }, //
                // Has system mapping to LatticeAccount and LatticeContact
                // (might not be same system)
                { Arrays.asList( //
                        fakeS3ImportSystem("AID1", true, null, null), //
                        fakeS3ImportSystem("AID2", false, "CID2", true) //
                ), "AID1", "CID2" }, //
                { Arrays.asList( //
                        fakeS3ImportSystem("AID1", false, "CID1", true), //
                        fakeS3ImportSystem("AID2", true, null, null) //
                ), "AID2", "CID1" }, //
                { Arrays.asList( //
                        fakeS3ImportSystem(null, null, "CID1", false), //
                        null, // Theoretical not expect to get null
                              // S3ImportSystem input
                        fakeS3ImportSystem("AID2", true, "CID2", true) //
                ), "AID2", "CID2" }, //

        };
    }

    private S3ImportSystem fakeS3ImportSystem(String acctSysId, Boolean mapToLAcct, String contSysId,
            Boolean mapToLCont) {
        S3ImportSystem sys = new S3ImportSystem();
        sys.setAccountSystemId(acctSysId);
        sys.setMapToLatticeAccount(mapToLAcct);
        sys.setContactSystemId(contSysId);
        sys.setMapToLatticeContact(mapToLCont);
        return sys;
    }

}
