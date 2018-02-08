package com.latticeengines.cdl.workflow.steps.process;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
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
import com.latticeengines.cdl.workflow.steps.process.StartProcessing;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessStepConfiguration;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;

public class StartProcessingTestNG {

    @Test(groups = { "unit" })
    public void testRebuildOnDLVersionTemplate() {
        DataCollection dataCollection = new DataCollection();
        dataCollection.setDataCloudBuildNumber("1001");
        DataCollectionProxy dataCollectionProxy = mock(DataCollectionProxy.class);
        when(dataCollectionProxy.getDefaultDataCollection(anyString())).thenReturn(dataCollection);

        StartProcessing startProcessing = new StartProcessing(dataCollectionProxy,
                CustomerSpace.parse(this.getClass().getSimpleName()));
        startProcessing.setExecutionContext(new ExecutionContext());
        ProcessStepConfiguration config = new ProcessStepConfiguration();
        config.setDataCloudBuildNumber("1000");
        startProcessing.setConfiguration(config);
        Set<BusinessEntity> entities = startProcessing.new RebuildOnDLVersionTemplate().getRebuildEntities();
        assertTrue(entities.contains(BusinessEntity.Account));
        assertEquals(entities.size(), 1);
    }

    @Test(groups = { "unit" })
    public void testRebuildOnDeleteJobTemplate() {
        Job job = new Job();
        job.setOutputs(ImmutableMap.<String, String> builder() //
                .put(WorkflowContextConstants.Outputs.IMPACTED_BUSINESS_ENTITIES, BusinessEntity.Contact.name()) //
                .build());

        List<Job> jobs = Arrays.asList(job);
        InternalResourceRestApiProxy internalResourceProxy = mock(InternalResourceRestApiProxy.class);
        when(internalResourceProxy.findJobsBasedOnActionIdsAndType(any(), any(), any())).thenReturn(jobs);

        StartProcessing startProcessing = new StartProcessing(internalResourceProxy,
                CustomerSpace.parse(this.getClass().getSimpleName()));
        startProcessing.setExecutionContext(new ExecutionContext());
        ProcessStepConfiguration config = new ProcessStepConfiguration();
        config.setActionIds(Collections.emptyList());
        startProcessing.setConfiguration(config);
        Set<BusinessEntity> entities = startProcessing.new RebuildOnDeleteJobTemplate().getRebuildEntities();
        assertTrue(entities.contains(BusinessEntity.Contact));
        assertEquals(entities.size(), 1);

        job = new Job();
        job.setOutputs(ImmutableMap.<String, String> builder() //
                .put(WorkflowContextConstants.Outputs.IMPACTED_BUSINESS_ENTITIES,
                        String.format("%s,%s,%s,%s", BusinessEntity.Account.name(), BusinessEntity.Contact.name(),
                                BusinessEntity.Product.name(), BusinessEntity.Transaction.name())) //
                .build());

        jobs = Arrays.asList(job);
        internalResourceProxy = mock(InternalResourceRestApiProxy.class);
        when(internalResourceProxy.findJobsBasedOnActionIdsAndType(any(), any(), any())).thenReturn(jobs);

        startProcessing = new StartProcessing(internalResourceProxy,
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
