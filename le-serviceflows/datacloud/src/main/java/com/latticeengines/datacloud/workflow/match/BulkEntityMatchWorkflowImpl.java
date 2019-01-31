package com.latticeengines.datacloud.workflow.match;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.workflow.match.steps.CleanupBulkEntityMatch;
import com.latticeengines.datacloud.workflow.match.steps.PrepareBulkEntityMatch;
import com.latticeengines.datacloud.workflow.match.steps.PublishEntity;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.BulkEntityMatchWorkflowConfiguration;
import com.latticeengines.serviceflows.workflow.match.BulkEntityMatchWorkflow;
import com.latticeengines.serviceflows.workflow.match.BulkMatchWorkflow;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

/*
 * Workflow to perform bulk entity match and the corresponding data preparation & cleanup
 */
@Component("bulkEntityMatchWorkflow")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class BulkEntityMatchWorkflowImpl extends AbstractWorkflow<BulkEntityMatchWorkflowConfiguration>
        implements BulkEntityMatchWorkflow {

    @Inject
    private PrepareBulkEntityMatch prepareBulkEntityMatch;

    @Inject
    private BulkMatchWorkflow bulkMatchWorkflow;

    @Inject
    private PublishEntity publishEntity;

    @Inject
    private CleanupBulkEntityMatch cleanupBulkEntityMatch;

    @Override
    public Workflow defineWorkflow(BulkEntityMatchWorkflowConfiguration config) {
        WorkflowBuilder workflowBuilder = new WorkflowBuilder(name(), config).next(prepareBulkEntityMatch);
        if (config.shouldPublishBeforeMatch()) {
            workflowBuilder.next(publishEntity);
        }
        if (config.shouldPerformBulkMatch()) {
            workflowBuilder.next(bulkMatchWorkflow);
        }
        if (config.shouldPublishAfterMatch()) {
            workflowBuilder.next(publishEntity);
        }
        return workflowBuilder.next(cleanupBulkEntityMatch).build();
    }
}
