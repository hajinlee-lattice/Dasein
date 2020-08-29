package com.latticeengines.cdl.workflow.steps.play;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import javax.inject.Inject;

import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.contactmaster.ContactMasterConstants;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.serviceflows.cdl.DeltaCampaignLaunchWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.LiveRampCampaignLaunchInitStepConfiguration;
import com.latticeengines.workflow.functionalframework.WorkflowTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-cdl-workflow-context.xml",
        "classpath:test-serviceflows-cdl-context.xml" })
public class LiveRampCampaignLaunchInitStepTestNG extends WorkflowTestNGBase {

    @Inject
    @Spy
    private LiveRampCampaignLaunchInitStep liveRampCampaignLaunchInitStep;

    private static final String MOCK_ADD_CONTACTS_TABLE = "FAKE_ADD_TABLE";
    private static final String MOCK_REMOVE_CONTACTS_TABLE = "FAKE_REMOVE_TABLE";

    private static final String ADD_CONTACTS_LOCATION = "ADD_CONTACTS_TEST_LOCATION";
    private static final String REMOVE_CONTACTS_LOCATION = "REMOVE_CONTACTS_TEST_LOCATION";

    @Override
    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        LiveRampCampaignLaunchInitStepConfiguration config = new LiveRampCampaignLaunchInitStepConfiguration();
        CustomerSpace customerSpace = CustomerSpace.parse(WORKFLOW_TENANT);
        config.setCustomerSpace(customerSpace);
        liveRampCampaignLaunchInitStep.setConfiguration(config);

        PlayLaunch playLaunch = new PlayLaunch();
        playLaunch.setAddContactsTable(MOCK_ADD_CONTACTS_TABLE);
        playLaunch.setRemoveContactsTable(MOCK_REMOVE_CONTACTS_TABLE);

        HdfsDataUnit fakeAddUnit = new HdfsDataUnit();
        fakeAddUnit.setPath(ADD_CONTACTS_LOCATION);
        HdfsDataUnit fakeRemoveUnit = new HdfsDataUnit();
        fakeRemoveUnit.setPath(REMOVE_CONTACTS_LOCATION);

        MockitoAnnotations.initMocks(this);
        Mockito.doReturn(playLaunch).when(liveRampCampaignLaunchInitStep).getPlayLaunchFromConfiguration();

        Mockito.doReturn(Boolean.toString(true)).when(
                liveRampCampaignLaunchInitStep)
                .getStringValueFromContext(DeltaCampaignLaunchWorkflowConfiguration.CREATE_ADD_CSV_DATA_FRAME);
        Mockito.doReturn(Boolean.toString(true)).when(liveRampCampaignLaunchInitStep)
                .getStringValueFromContext(DeltaCampaignLaunchWorkflowConfiguration.CREATE_DELETE_CSV_DATA_FRAME);
        Mockito.doReturn(fakeAddUnit).when(liveRampCampaignLaunchInitStep)
                .getHdfsDataUnitFromTable(MOCK_ADD_CONTACTS_TABLE);
        Mockito.doReturn(fakeRemoveUnit).when(liveRampCampaignLaunchInitStep)
                .getHdfsDataUnitFromTable(MOCK_REMOVE_CONTACTS_TABLE);
    }

    @Test(groups = "functional")
    public void testExecute() {
        ExecutionContext executionContext = new ExecutionContext();
        liveRampCampaignLaunchInitStep.setExecutionContext(executionContext);

        liveRampCampaignLaunchInitStep.execute();

        testLiveRampDisplayNames();
        testLiveRampAvroPaths();
    }

    private void testLiveRampDisplayNames() {
        Map<String, String> displayNames = liveRampCampaignLaunchInitStep
                .getMapObjectFromContext(
                        LiveRampCampaignLaunchInitStep.RECOMMENDATION_CONTACT_DISPLAY_NAMES,
                        String.class, String.class);

        assertTrue(displayNames.containsKey(ContactMasterConstants.TPS_ATTR_RECORD_ID));
        assertEquals(LiveRampCampaignLaunchInitStep.RECORD_ID_DISPLAY_NAME, displayNames.get(ContactMasterConstants.TPS_ATTR_RECORD_ID));
    }

    private void testLiveRampAvroPaths() {
        String actualAddContactsLocation = liveRampCampaignLaunchInitStep
                .getStringValueFromContext(DeltaCampaignLaunchWorkflowConfiguration.ADD_CSV_EXPORT_AVRO_HDFS_FILEPATH);
        String actualRemoveContactsLocation = liveRampCampaignLaunchInitStep
                .getStringValueFromContext(
                        DeltaCampaignLaunchWorkflowConfiguration.DELETE_CSV_EXPORT_AVRO_HDFS_FILEPATH);

        assertEquals(PathUtils.toAvroGlob(ADD_CONTACTS_LOCATION), actualAddContactsLocation);
        assertEquals(PathUtils.toAvroGlob(REMOVE_CONTACTS_LOCATION), actualRemoveContactsLocation);
    }
}
