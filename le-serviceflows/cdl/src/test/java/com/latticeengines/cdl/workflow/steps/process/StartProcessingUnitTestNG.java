package com.latticeengines.cdl.workflow.steps.process;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.springframework.batch.item.ExecutionContext;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.ChoreographerContext;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.CleanupActionConfiguration;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessStepConfiguration;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.cdl.ActionProxy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

public class StartProcessingUnitTestNG {

    @Test(groups = { "unit" })
    public void testRebuildOnDLVersionChange() {
        DataCollection dataCollection = new DataCollection();
        dataCollection.setDataCloudBuildNumber("1001");
        DataCollectionProxy dataCollectionProxy = mock(DataCollectionProxy.class);
        when(dataCollectionProxy.getDefaultDataCollection(anyString())).thenReturn(dataCollection);

        StartProcessing startProcessing = new StartProcessing(dataCollectionProxy, null, null,
                CustomerSpace.parse(this.getClass().getSimpleName()));
        startProcessing.setExecutionContext(new ExecutionContext());
        ProcessStepConfiguration config = new ProcessStepConfiguration();
        config.setDataCloudBuildNumber("1000");
        startProcessing.setConfiguration(config);

        StartProcessing spy = spy(startProcessing);
        doReturn(null).when(spy).getImpactedEntities();
        doReturn(true).when(spy).hasAccountBatchStore();
        doReturn(Collections.emptyList()).when(spy).getActions();
        doReturn(Collections.emptyList()).when(spy).getRatingRelatedActions(any());
        doReturn(null).when(spy).getActionImpactedSegmentNames(any());
        doReturn(null).when(spy).getActionImpactedAIEngineIds(any(), any());
        doReturn(null).when(spy).getActionImpactedRuleEngineIds(any(), any());
        spy.setGrapherContext();
        ChoreographerContext context = spy.getObjectFromContext(BaseWorkflowStep.CHOREOGRAPHER_CONTEXT_KEY,
                ChoreographerContext.class);
        assertTrue(context.isDataCloudChanged());
    }

    @Test(groups = { "unit" })
    public void testRebuildOnDeleteJobTemplate() {
        Job job = new Job();
        job.setOutputs(ImmutableMap.<String, String> builder() //
                .put(WorkflowContextConstants.Outputs.IMPACTED_BUSINESS_ENTITIES,
                        JsonUtils.serialize(Arrays.asList(BusinessEntity.Contact.name()))) //
                .build());

        List<Job> jobs = Arrays.asList(job);
        InternalResourceRestApiProxy internalResourceProxy = mock(InternalResourceRestApiProxy.class);
        when(internalResourceProxy.findJobsBasedOnActionIdsAndType(any(), any(), any())).thenReturn(jobs);

        Action action = new Action();
        action.setType(ActionType.CDL_OPERATION_WORKFLOW);
        action.setActionInitiator("Test_Action_Initiator");
        CleanupActionConfiguration cleanupActionConfiguration = new CleanupActionConfiguration();
        cleanupActionConfiguration.addImpactEntity(BusinessEntity.Contact);
        action.setActionConfiguration(cleanupActionConfiguration);
        List<Action> actions = Arrays.asList(action);
        ActionProxy actionProxy = mock(ActionProxy.class);
        when(actionProxy.getActionsByPids(any(), any())).thenReturn(actions);

        StartProcessing startProcessing = new StartProcessing(null, internalResourceProxy, actionProxy,
                CustomerSpace.parse(this.getClass().getSimpleName()));
        startProcessing.setExecutionContext(new ExecutionContext());
        ProcessStepConfiguration config = new ProcessStepConfiguration();
        config.setActionIds(Arrays.asList(1111L));
        startProcessing.setConfiguration(config);
        Set<BusinessEntity> entities = startProcessing.new RebuildOnDeleteJobTemplate().getRebuildEntities();
        assertTrue(entities.contains(BusinessEntity.Contact));
        assertEquals(entities.size(), 1);

        job = new Job();
        job.setOutputs(ImmutableMap.<String, String> builder() //
                .put(WorkflowContextConstants.Outputs.IMPACTED_BUSINESS_ENTITIES,
                        JsonUtils.serialize(Arrays.asList(BusinessEntity.Account.name(), BusinessEntity.Contact.name(),
                                BusinessEntity.Product.name(), BusinessEntity.Transaction.name()))) //
                .build());

        jobs = Arrays.asList(job);
        internalResourceProxy = mock(InternalResourceRestApiProxy.class);
        when(internalResourceProxy.findJobsBasedOnActionIdsAndType(any(), any(), any())).thenReturn(jobs);

        cleanupActionConfiguration = new CleanupActionConfiguration();
        cleanupActionConfiguration.addImpactEntity(BusinessEntity.Account);
        cleanupActionConfiguration.addImpactEntity(BusinessEntity.Contact);
        cleanupActionConfiguration.addImpactEntity(BusinessEntity.Product);
        cleanupActionConfiguration.addImpactEntity(BusinessEntity.Transaction);
        action.setActionConfiguration(cleanupActionConfiguration);
        actionProxy = mock(ActionProxy.class);
        when(actionProxy.getActionsByPids(any(), any())).thenReturn(actions);

        startProcessing = new StartProcessing(null, internalResourceProxy, actionProxy,
                CustomerSpace.parse(this.getClass().getSimpleName()));
        startProcessing.setExecutionContext(new ExecutionContext());

        startProcessing.setConfiguration(config);
        entities = startProcessing.new RebuildOnDeleteJobTemplate().getRebuildEntities();
        assertTrue(entities.contains(BusinessEntity.Account));
        assertTrue(entities.contains(BusinessEntity.Contact));
        assertTrue(entities.contains(BusinessEntity.Product));
        assertTrue(entities.contains(BusinessEntity.Transaction));
        assertEquals(entities.size(), 4);
    }
}
