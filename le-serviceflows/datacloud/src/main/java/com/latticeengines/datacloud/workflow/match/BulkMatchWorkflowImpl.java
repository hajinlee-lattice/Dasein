package com.latticeengines.datacloud.workflow.match;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.workflow.match.listeners.UpdateFailedMatchListener;
import com.latticeengines.datacloud.workflow.match.steps.ParallelBlockExecution;
import com.latticeengines.datacloud.workflow.match.steps.PrepareBulkMatchInput;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.BulkMatchWorkflowConfiguration;
import com.latticeengines.serviceflows.workflow.export.ExportData;
import com.latticeengines.serviceflows.workflow.match.BulkMatchWorkflow;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("bulkMatchWorkflow")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class BulkMatchWorkflowImpl extends AbstractWorkflow<BulkMatchWorkflowConfiguration>
        implements BulkMatchWorkflow {

    @Autowired
    private PrepareBulkMatchInput prepareBulkMatchInput;

    @Autowired
    private ParallelBlockExecution parallelBlockExecution;

    @Autowired
    private ExportData exportData;

    @Autowired
    private UpdateFailedMatchListener updateFailedMatchListener;

    @Override
    public Workflow defineWorkflow(BulkMatchWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(prepareBulkMatchInput) //
                .next(parallelBlockExecution) //
                .next(exportData) //
                .listener(updateFailedMatchListener) //
                .build();
    }

}
