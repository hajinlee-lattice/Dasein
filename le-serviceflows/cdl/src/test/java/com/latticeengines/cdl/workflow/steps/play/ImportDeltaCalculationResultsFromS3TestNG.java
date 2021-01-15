package com.latticeengines.cdl.workflow.steps.play;

import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.assertEquals;

import java.util.List;

import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.test.context.ContextConfiguration;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.LaunchBaseType;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.cdl.channel.AudienceType;
import com.latticeengines.domain.exposed.pls.cdl.channel.ChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.LinkedInChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.MediaMathChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.OutreachChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.SalesforceChannelConfig;
import com.latticeengines.domain.exposed.serviceflows.cdl.DeltaCampaignLaunchWorkflowConfiguration;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.workflow.functionalframework.WorkflowTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-cdl-workflow-context.xml",
        "classpath:test-serviceflows-cdl-context.xml" })
public class ImportDeltaCalculationResultsFromS3TestNG extends WorkflowTestNGBase {

    private static final String ADD_ACCOUNTS_LOCATION = "ADD_ACCOUNTS_LOCATION";
    private static final String ADD_CONTACTS_LOCATION = "ADD_CONTACTS_LOCATION";
    private static final String REMOVE_ACCOUNTS_LOCATION = "REMOVE_ACCOUNTS_LOCATION";
    private static final String REMOVE_CONTACTS_LOCATION = "REMOVE_CONTACTS_LOCATION";
    private static final String COMPLETE_CONTACTS_LOCATION = "COMPLETE_CONTACTS_LOCATION";

    @InjectMocks
    private ImportDeltaCalculationResultsFromS3 importDeltaCalculationResultsFromS3;

    @Mock
    private PlayProxy playProxy;

    private PlayLaunch playLaunch;
    private ExecutionContext executionContext;

    @Override
    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);

        playLaunch = new PlayLaunch();

        executionContext = new ExecutionContext();
        importDeltaCalculationResultsFromS3.setExecutionContext(executionContext);

        Mockito.doReturn(playLaunch).when(playProxy).getPlayLaunch(any(), any(), any());
    }

    @Test(groups = "functional")
    public void testGetMetadataTableNamesWithLiveRamp() {
        ChannelConfig channelConfig = new MediaMathChannelConfig();
        playLaunch.setChannelConfig(channelConfig);
        List<String> result = null;

        // All Accounts and Contacts enabled
        clearExecutionContext();
        reconfigureCurrentPlayLaunchForAccounts(true, true);
        reconfigureCurrentPlayLaunchForContacts(true, true, true);
        result = importDeltaCalculationResultsFromS3.getMetadataTableNames(new CustomerSpace(), "", "");
        assertEquals(2, result.size());
        assertDataFrameResults(true, true, false);
        assertDataFrameCount(String.valueOf(2));

        // Accounts disabled with All Contacts
        clearExecutionContext();
        reconfigureCurrentPlayLaunchForAccounts(false, false);
        reconfigureCurrentPlayLaunchForContacts(true, true, false);
        result = importDeltaCalculationResultsFromS3.getMetadataTableNames(new CustomerSpace(), "", "");
        assertEquals(2, result.size());
        assertDataFrameResults(true, true, false);
        assertDataFrameCount(String.valueOf(2));

        // Accounts disabled with only add Contacts
        clearExecutionContext();
        reconfigureCurrentPlayLaunchForAccounts(false, false);
        reconfigureCurrentPlayLaunchForContacts(true, false, false);
        result = importDeltaCalculationResultsFromS3.getMetadataTableNames(new CustomerSpace(), "", "");
        assertEquals(1, result.size());
        assertDataFrameResults(true, false, false);
        assertDataFrameCount(String.valueOf(1));

        // Accounts disabled with only delete Contacts
        clearExecutionContext();
        reconfigureCurrentPlayLaunchForAccounts(false, false);
        reconfigureCurrentPlayLaunchForContacts(false, true, false);
        result = importDeltaCalculationResultsFromS3.getMetadataTableNames(new CustomerSpace(), "", "");
        assertEquals(1, result.size());
        assertDataFrameResults(false, true, false);
        assertDataFrameCount(String.valueOf(1));

        // Accounts disabled and Contacts disabled
        clearExecutionContext();
        reconfigureCurrentPlayLaunchForAccounts(false, false);
        reconfigureCurrentPlayLaunchForContacts(false, false, false);
        try {
            result = importDeltaCalculationResultsFromS3.getMetadataTableNames(new CustomerSpace(), "", "");
            fail("Expected message with \"There is nothing to be launched.\"");
        } catch (Exception e) {
            assertDataFrameResults(false, false, false);
            assertDataFrameCount(String.valueOf(0));
        }
    }

    @Test(groups = "functional")
    public void testGetMetadataTableNamesWithAccounts() {
        LinkedInChannelConfig channelConfig = new LinkedInChannelConfig();
        channelConfig.setAudienceType(AudienceType.ACCOUNTS);
        playLaunch.setChannelConfig(channelConfig);
        List<String> result = null;

        // Enable all contacts
        reconfigureCurrentPlayLaunchForContacts(true, true, true);

        // All Accounts
        clearExecutionContext();
        reconfigureCurrentPlayLaunchForAccounts(true, true);
        result = importDeltaCalculationResultsFromS3.getMetadataTableNames(new CustomerSpace(), "", "");
        assertEquals(5, result.size());
        assertDataFrameResults(true, true, true);
        assertDataFrameCount(String.valueOf(3));

        // Only Add Accounts
        clearExecutionContext();
        reconfigureCurrentPlayLaunchForAccounts(true, false);
        result = importDeltaCalculationResultsFromS3.getMetadataTableNames(new CustomerSpace(), "", "");
        assertEquals(3, result.size());
        assertDataFrameResults(true, false, true);
        assertDataFrameCount(String.valueOf(2));

        // Only Remove Accounts
        clearExecutionContext();
        reconfigureCurrentPlayLaunchForAccounts(false, true);
        result = importDeltaCalculationResultsFromS3.getMetadataTableNames(new CustomerSpace(), "", "");
        assertEquals(2, result.size());
        assertDataFrameResults(false, true, false);
        assertDataFrameCount(String.valueOf(1));

        // Disable All Contacts
        reconfigureCurrentPlayLaunchForContacts(false, false, false);

        // All Accounts enabled
        clearExecutionContext();
        reconfigureCurrentPlayLaunchForAccounts(true, true);
        result = importDeltaCalculationResultsFromS3.getMetadataTableNames(new CustomerSpace(), "", "");
        assertEquals(2, result.size());
        assertDataFrameResults(true, true, true);
        assertDataFrameCount(String.valueOf(3));

        // Only Add Accounts enabled
        clearExecutionContext();
        reconfigureCurrentPlayLaunchForAccounts(true, false);
        result = importDeltaCalculationResultsFromS3.getMetadataTableNames(new CustomerSpace(), "", "");
        assertEquals(1, result.size());
        assertDataFrameResults(true, false, true);
        assertDataFrameCount(String.valueOf(2));

        // Only Remove Accounts enabled
        clearExecutionContext();
        reconfigureCurrentPlayLaunchForAccounts(false, true);
        result = importDeltaCalculationResultsFromS3.getMetadataTableNames(new CustomerSpace(), "", "");
        assertEquals(1, result.size());
        assertDataFrameResults(false, true, false);
        assertDataFrameCount(String.valueOf(1));

        // Accounts disabled
        clearExecutionContext();
        reconfigureCurrentPlayLaunchForAccounts(false, false);
        try {
            result = importDeltaCalculationResultsFromS3.getMetadataTableNames(new CustomerSpace(), "", "");
            fail("Expected message with \"There is nothing to be launched.\"");
        } catch (Exception e) {
            assertDataFrameResults(false, false, false);
            assertDataFrameCount(String.valueOf(0));
        }

    }

    @Test(groups = "functional")
    public void testGetMetadataTableNamesWithContacts() {
        LinkedInChannelConfig channelConfig = new LinkedInChannelConfig();
        channelConfig.setAudienceType(AudienceType.CONTACTS);
        playLaunch.setChannelConfig(channelConfig);
        List<String> result = null;

        // All Accounts and Contacts enabled
        clearExecutionContext();
        reconfigureCurrentPlayLaunchForAccounts(true, true);
        reconfigureCurrentPlayLaunchForContacts(true, true, true);
        clearExecutionContext();
        result = importDeltaCalculationResultsFromS3.getMetadataTableNames(new CustomerSpace(), "", "");
        assertEquals(5, result.size());
        assertDataFrameResults(true, true, true);
        assertDataFrameCount(String.valueOf(3));

        // Only Add Contacts and Complete Contacts enabled
        clearExecutionContext();
        reconfigureCurrentPlayLaunchForAccounts(true, true);
        reconfigureCurrentPlayLaunchForContacts(true, false, true);
        clearExecutionContext();
        result = importDeltaCalculationResultsFromS3.getMetadataTableNames(new CustomerSpace(), "", "");
        assertEquals(3, result.size());
        assertDataFrameResults(true, false, true);
        assertDataFrameCount(String.valueOf(2));

        // Only Delete Contacts
        clearExecutionContext();
        reconfigureCurrentPlayLaunchForAccounts(true, true);
        reconfigureCurrentPlayLaunchForContacts(false, true, false);
        clearExecutionContext();
        result = importDeltaCalculationResultsFromS3.getMetadataTableNames(new CustomerSpace(), "", "");
        assertEquals(2, result.size());
        assertDataFrameResults(false, true, false);
        assertDataFrameCount(String.valueOf(1));

        // No Add or Remove Accounts
        reconfigureCurrentPlayLaunchForAccounts(false, false);

        // All contacts
        clearExecutionContext();
        reconfigureCurrentPlayLaunchForContacts(true, true, true);
        Assert.assertThrows(() -> importDeltaCalculationResultsFromS3.getMetadataTableNames(new CustomerSpace(), "", ""));

        // Only Add and Complete Contacts
        clearExecutionContext();
        reconfigureCurrentPlayLaunchForContacts(true, false, true);
        Assert.assertThrows(() -> importDeltaCalculationResultsFromS3.getMetadataTableNames(new CustomerSpace(), "", ""));

        // Only Add Contacts
        clearExecutionContext();
        reconfigureCurrentPlayLaunchForContacts(true, false, false);
        Assert.assertThrows(() -> importDeltaCalculationResultsFromS3.getMetadataTableNames(new CustomerSpace(), "", ""));

        // Only Complete Contacts
        clearExecutionContext();
        reconfigureCurrentPlayLaunchForContacts(false, false, true);
        try {
            importDeltaCalculationResultsFromS3.getMetadataTableNames(new CustomerSpace(), "", "");
            fail("Expected message with \"There is nothing to be launched.\"");
        } catch (Exception e) {
            assertDataFrameResults(false, false, false);
            assertDataFrameCount(String.valueOf(0));
        }

        // Only Remove Contacts
        clearExecutionContext();
        reconfigureCurrentPlayLaunchForContacts(false, true, false);
        Assert.assertThrows(() -> importDeltaCalculationResultsFromS3.getMetadataTableNames(new CustomerSpace(), "", ""));

        // Only Remove Contacts
        clearExecutionContext();
        reconfigureCurrentPlayLaunchForContacts(false, false, false);
        try {
            importDeltaCalculationResultsFromS3.getMetadataTableNames(new CustomerSpace(), "", "");
            fail("Expected message with \"There is nothing to be launched.\"");
        } catch (Exception e) {
            assertDataFrameResults(false, false, false);
            assertDataFrameCount(String.valueOf(0));
        }
    }

    @Test(groups = "functional")
    public void testGetMetadataTableNamesWithSalesForce() {
        ChannelConfig channelConfig = new SalesforceChannelConfig();
        playLaunch.setChannelConfig(channelConfig);
        playLaunch.setDestinationSysName(CDLExternalSystemName.Salesforce);
        List<String> result;
        // All Accounts and Contacts enabled
        clearExecutionContext();
        reconfigureCurrentPlayLaunchForAccounts(true, true);
        reconfigureCurrentPlayLaunchForContacts(true, true, true);
        result = importDeltaCalculationResultsFromS3.getMetadataTableNames(new CustomerSpace(), "", "");
        assertEquals(3, result.size());
        assertDataFrameResults(true, false, true);
    }

    @Test(groups = "functional")
    public void testHasOutreachTaskDescription() {
        OutreachChannelConfig outreachConfig = new OutreachChannelConfig();
        outreachConfig.setLaunchBaseType(LaunchBaseType.TASK);
        outreachConfig.setTaskDescription("Test task!");

        boolean result = importDeltaCalculationResultsFromS3.hasOutreachTaskDescription(outreachConfig);
        Assert.assertTrue(result);
    }

    @Test(groups = "functional")
    public void testEmptyOutreachTaskDescription() {
        OutreachChannelConfig outreachConfig = new OutreachChannelConfig();
        outreachConfig.setLaunchBaseType(LaunchBaseType.TASK);
        outreachConfig.setTaskDescription("");

        boolean result = importDeltaCalculationResultsFromS3.hasOutreachTaskDescription(outreachConfig);
        Assert.assertFalse(result);
    }

    private void clearExecutionContext() {
        executionContext.entrySet().clear();
    }

    private void reconfigureCurrentPlayLaunchForAccounts(boolean setAddAccounts, boolean setRemoveAccounts) {
        if (setAddAccounts) {
            playLaunch.setAddAccountsTable(ADD_ACCOUNTS_LOCATION);
        } else {
            playLaunch.setAddAccountsTable(null);
        }

        if (setRemoveAccounts) {
            playLaunch.setRemoveAccountsTable(REMOVE_ACCOUNTS_LOCATION);
        } else {
            playLaunch.setRemoveAccountsTable(null);
        }
    }

    private void reconfigureCurrentPlayLaunchForContacts(boolean setAddContacts, boolean setRemoveContacts,
            boolean setCompleteContacts) {
        if (setAddContacts) {
            playLaunch.setAddContactsTable(ADD_CONTACTS_LOCATION);
        } else {
            playLaunch.setAddContactsTable(null);
        }

        if (setRemoveContacts) {
            playLaunch.setRemoveContactsTable(REMOVE_CONTACTS_LOCATION);
        } else {
            playLaunch.setRemoveContactsTable(null);
        }

        if (setCompleteContacts) {
            playLaunch.setCompleteContactsTable(COMPLETE_CONTACTS_LOCATION);
        } else {
            playLaunch.setCompleteContactsTable(null);
        }
    }

    private void assertDataFrameResults(boolean expectedCreate, boolean expectedDelete,
            boolean expectedCreateRecommendation) {
        boolean createAddCsvDataFrame = Boolean.toString(true).equals(importDeltaCalculationResultsFromS3
                .getStringValueFromContext(DeltaCampaignLaunchWorkflowConfiguration.CREATE_ADD_CSV_DATA_FRAME));
        boolean createRemoveCsvDataFrame = Boolean.toString(true).equals(importDeltaCalculationResultsFromS3
                .getStringValueFromContext(DeltaCampaignLaunchWorkflowConfiguration.CREATE_DELETE_CSV_DATA_FRAME));
        boolean createRecommendationCsvDataFrame = Boolean.toString(true).equals(importDeltaCalculationResultsFromS3
                .getStringValueFromContext(DeltaCampaignLaunchWorkflowConfiguration.CREATE_RECOMMENDATION_DATA_FRAME));

        assertEquals(expectedCreate, createAddCsvDataFrame);
        assertEquals(expectedDelete, createRemoveCsvDataFrame);
        assertEquals(expectedCreateRecommendation, createRecommendationCsvDataFrame);
    }

    private void assertDataFrameCount(String expectedCount) {
        String actualCount = importDeltaCalculationResultsFromS3
                .getStringValueFromContext(DeltaCampaignLaunchWorkflowConfiguration.DATA_FRAME_NUM);
        assertEquals(expectedCount, actualCount);
    }
}
