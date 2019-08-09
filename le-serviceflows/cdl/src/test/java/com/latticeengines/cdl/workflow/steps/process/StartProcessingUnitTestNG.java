package com.latticeengines.cdl.workflow.steps.process;

import static com.latticeengines.domain.exposed.query.BusinessEntity.Account;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Contact;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.batch.item.ExecutionContext;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.ChoreographerContext;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatusDetail;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.CleanupActionConfiguration;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessStepConfiguration;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.cdl.ActionProxy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

public class StartProcessingUnitTestNG {
    @Test(groups = { "unit" }, dataProvider = "DataCloudBuildNumber")
    public void testRebuildOnDataCloudVersionChange(String dcBuildNumber) {
        Long actionPid = 1L, ownerId = 100L;

        DataCollectionStatus dataCollectionStatus = new DataCollectionStatus();
        dataCollectionStatus.setDataCloudBuildNumber(dcBuildNumber);
        DataCollectionProxy dataCollectionProxy = mock(DataCollectionProxy.class);
        when(dataCollectionProxy.getOrCreateDataCollectionStatus(anyString(), any())).thenReturn(dataCollectionStatus);
        Action mockAction = new Action();
        mockAction.setPid(actionPid);
        mockAction.setOwnerId(ownerId);
        ActionProxy actionProxy = mock(ActionProxy.class);
        when(actionProxy.createAction(anyString(), any())).thenReturn(mockAction);

        StartProcessing startProcessing = new StartProcessing(dataCollectionProxy, actionProxy,
                CustomerSpace.parse(this.getClass().getSimpleName()));
        startProcessing.setExecutionContext(new ExecutionContext());
        List<Long> actionIds = startProcessing.getListObjectFromContext(BaseWorkflowStep.SYSTEM_ACTION_IDS, Long.class);
        assertTrue(CollectionUtils.isEmpty(actionIds));
        actionIds = new ArrayList<>();
        actionIds.add(actionPid);
        ProcessStepConfiguration config = new ProcessStepConfiguration();
        config.setActionIds(new ArrayList<>());
        config.setDataCloudBuildNumber("2.13.1.1234567");
        config.setInputProperties(new HashMap<>());
        startProcessing.setConfiguration(config);

        StartProcessing spy = spy(startProcessing);
        doReturn(null).when(spy).getEntitiesShouldRebuildByActions();
        doReturn(true).when(spy).hasAccountBatchStore();
        doReturn(Collections.emptyList()).when(spy).getActions();
        doReturn(null).when(spy).getActionImpactedSegmentNames(any());
        doReturn(null).when(spy).getActionImpactedEngineIds(any());
        spy.putObjectInContext("CDL_COLLECTION_STATUS", dataCollectionStatus);
        spy.setGrapherContext();
        ChoreographerContext context = spy.getObjectFromContext(BaseWorkflowStep.CHOREOGRAPHER_CONTEXT_KEY,
                ChoreographerContext.class);
        assertTrue(context.isDataCloudChanged());
        spy.putObjectInContext(BaseWorkflowStep.SYSTEM_ACTION_IDS, actionIds);
        actionIds = startProcessing.getListObjectFromContext(BaseWorkflowStep.SYSTEM_ACTION_IDS, Long.class);
        List<Long> actionIdsArr =
                JsonUtils.convertList(JsonUtils.deserialize(config.getInputProperties().get(WorkflowContextConstants.Inputs.ACTION_IDS),
                        List.class), Long.class);
        assertTrue(CollectionUtils.isNotEmpty(actionIds));
        assertEquals(actionIds.size(), 1);
        assertEquals(actionIds.get(0), actionPid);
        assertEquals(actionIds, actionIdsArr);
    }

    @DataProvider(name = "DataCloudBuildNumber")
    private Object[][] getDCBuildNumber(){
        return new Object[][] {
                { "2.12.8.7654321" }, //
                { DataCollectionStatusDetail.NOT_SET } //
        };
    };

    @Test(groups = { "unit" })
    public void testRebuildOnDeleteJobTemplate() {
        Job job = new Job();
        job.setOutputs(ImmutableMap.<String, String> builder() //
                .put(WorkflowContextConstants.Outputs.IMPACTED_BUSINESS_ENTITIES,
                        JsonUtils.serialize(Collections.singletonList(Contact.name()))) //
                .build());

        List<Job> jobs = Collections.singletonList(job);
        Action action = new Action();
        action.setType(ActionType.CDL_OPERATION_WORKFLOW);
        action.setActionInitiator("Test_Action_Initiator");
        CleanupActionConfiguration cleanupActionConfiguration = new CleanupActionConfiguration();
        cleanupActionConfiguration.addImpactEntity(Contact);
        action.setActionConfiguration(cleanupActionConfiguration);
        List<Action> actions = Collections.singletonList(action);
        ActionProxy actionProxy = mock(ActionProxy.class);
        when(actionProxy.getActionsByPids(any(), any())).thenReturn(actions);

        StartProcessing startProcessing = new StartProcessing(null, actionProxy,
                CustomerSpace.parse(this.getClass().getSimpleName()));
        startProcessing.setExecutionContext(new ExecutionContext());
        ProcessStepConfiguration config = new ProcessStepConfiguration();
        config.setInputProperties(new HashMap<>());
        config.setActionIds(Collections.singletonList(1111L));
        startProcessing.setConfiguration(config);
        Set<BusinessEntity> entities = startProcessing.new RebuildOnDeleteJobTemplate().getRebuildEntities();
        assertTrue(entities.contains(Contact));
        assertEquals(entities.size(), 1);

        job = new Job();
        job.setOutputs(ImmutableMap.<String, String> builder() //
                .put(WorkflowContextConstants.Outputs.IMPACTED_BUSINESS_ENTITIES,
                        JsonUtils.serialize(Arrays.asList(Account.name(), Contact.name(),
                                BusinessEntity.Product.name(), BusinessEntity.Transaction.name()))) //
                .build());

        jobs = Collections.singletonList(job);
        cleanupActionConfiguration = new CleanupActionConfiguration();
        cleanupActionConfiguration.addImpactEntity(Account);
        cleanupActionConfiguration.addImpactEntity(Contact);
        cleanupActionConfiguration.addImpactEntity(BusinessEntity.Product);
        cleanupActionConfiguration.addImpactEntity(BusinessEntity.Transaction);
        action.setActionConfiguration(cleanupActionConfiguration);
        actionProxy = mock(ActionProxy.class);
        when(actionProxy.getActionsByPids(any(), any())).thenReturn(actions);

        startProcessing = new StartProcessing(null, actionProxy,
                CustomerSpace.parse(this.getClass().getSimpleName()));
        startProcessing.setExecutionContext(new ExecutionContext());

        startProcessing.setConfiguration(config);
        entities = startProcessing.new RebuildOnDeleteJobTemplate().getRebuildEntities();
        assertTrue(entities.contains(Account));
        assertTrue(entities.contains(Contact));
        assertTrue(entities.contains(BusinessEntity.Product));
        assertTrue(entities.contains(BusinessEntity.Transaction));
        assertEquals(entities.size(), 4);
    }

//    @Test(groups = "unit", dataProvider = "bumpEntityMatchVersion", enabled = false)
//    private void testBumpEntityMatchVersion(BusinessEntity[] entitiesWithImport, BusinessEntity[] rebuildEntities,
//            List<EntityMatchEnvironment> expectedEnvironments) {
//        StartProcessing step = mockStepImportAndRebuild(entitiesWithImport, rebuildEntities);
//        Assert.assertEquals(new HashSet<>(step.getEnvironmentsToBumpVersion()), new HashSet<>(expectedEnvironments));
//    }
//
//    @DataProvider(name = "bumpEntityMatchVersion")
//    private Object[][] provideBumpVersionData() {
//        return new Object[][] { //
//                /*
//                 * NO account import, not rebuilding account => does not bump up any env
//                 */
//                { null, null, Collections.emptyList() }, //
//                { new BusinessEntity[] { Contact }, null, Collections.emptyList() }, //
//                { new BusinessEntity[] { Contact, Product }, new BusinessEntity[] { Contact },
//                        Collections.emptyList() }, //
//                /*
//                 * HAS account import, not rebuilding account => bump up STAGING env
//                 */
//                { new BusinessEntity[] { Account }, null, staging() }, //
//                { new BusinessEntity[] { Account, Contact }, null, staging() }, //
//                { new BusinessEntity[] { Account }, new BusinessEntity[] { Contact, Product },
//                        Collections.singletonList(STAGING) }, //
//                { new BusinessEntity[] { Account, Product }, new BusinessEntity[] { Contact },
//                        Collections.singletonList(STAGING) }, //
//                /*
//                 * NO account import, rebuild account => bump up STAGING & SERVING env
//                 */
//                { null, new BusinessEntity[] { Account }, stagingAndServing() }, //
//                { new BusinessEntity[] { Contact, Product }, new BusinessEntity[] { Account }, stagingAndServing() }, //
//                { new BusinessEntity[] { Contact, Product }, new BusinessEntity[] { Account, Contact, Product },
//                        stagingAndServing() }, //
//                /*
//                 * HAS account import, rebuild account => bump up STAGING & SERVING env
//                 */
//                { new BusinessEntity[] { Account }, new BusinessEntity[] { Account }, stagingAndServing() }, //
//                { new BusinessEntity[] { Account }, new BusinessEntity[] { Account, Contact }, stagingAndServing() }, //
//                { new BusinessEntity[] { Account, Product }, new BusinessEntity[] { Account, Contact },
//                        stagingAndServing() }, //
//                { new BusinessEntity[] { Account, Product, Contact },
//                        new BusinessEntity[] { Account, Contact, Product }, stagingAndServing() }, //
//        };
//    }
//
//    /*
//     * mock import map for specified entities and configure rebuild entity set
//     * properly. also set enable entity match flag to true.
//     */
//    private StartProcessing mockStepImportAndRebuild(BusinessEntity[] entitiesWithImport,
//            BusinessEntity[] rebuildEntities) {
//        StartProcessing step = new StartProcessing();
//        step.setExecutionContext(new ExecutionContext());
//        if (entitiesWithImport != null) {
//            Map<String, ArrayList<DataFeedImport>> importMap = Arrays.stream(entitiesWithImport) //
//                    .map(Enum::name) //
//                    .map(entity -> Pair.of(entity, new ArrayList<DataFeedImport>())) //
//                    .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
//            step.putObjectInContext(StartProcessing.CONSOLIDATE_INPUT_IMPORTS, importMap);
//        }
//        step.setConfiguration(new ProcessStepConfiguration());
//        step.getConfiguration().setEntityMatchEnabled(true);
//        if (rebuildEntities != null) {
//            step.getConfiguration().setRebuildEntities(Arrays.stream(rebuildEntities).collect(Collectors.toSet()));
//        }
//        return step;
//    }
//
//    private List<EntityMatchEnvironment> staging() {
//        return Collections.singletonList(STAGING);
//    }
//
//    private List<EntityMatchEnvironment> stagingAndServing() {
//        return Arrays.asList(SERVING, STAGING);
//    }
}
