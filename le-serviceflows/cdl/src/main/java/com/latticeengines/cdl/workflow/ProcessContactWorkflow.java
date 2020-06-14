package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.maintenance.SoftDeleteContactWrapper;
import com.latticeengines.cdl.workflow.steps.merge.MergeContactWrapper;
import com.latticeengines.cdl.workflow.steps.rebuild.CalcContactStats;
import com.latticeengines.cdl.workflow.steps.rebuild.RemoveOrphanContact;
import com.latticeengines.cdl.workflow.steps.reset.ResetContact;
import com.latticeengines.cdl.workflow.steps.validations.ValidateContactBatchStore;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.ProcessContactWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("processContactWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ProcessContactWorkflow extends AbstractWorkflow<ProcessContactWorkflowConfiguration> {

    @Inject
    private SoftDeleteContactWrapper softDeleteContactWrapper;

    @Inject
    private MergeContactWrapper mergeContactWrapper;

    @Inject
    private ValidateContactBatchStore validateContactBatchStore;

    @Inject
    private RemoveOrphanContact removeOrphanContact;

    @Inject
    private CalcContactStats calcContactStats;

    @Inject
    private UpdateContactWorkflow updateContactWorkflow;

    @Inject
    private RebuildContactWorkflow rebuildContactWorkflow;

    @Inject
    private ResetContact resetContact;

    @Value("${cdl.use.changelist}")
    private boolean useChangeList;

    @Override
    public Workflow defineWorkflow(ProcessContactWorkflowConfiguration config) {
        WorkflowBuilder builder = new WorkflowBuilder(name(), config) //
                .next(softDeleteContactWrapper) //
                .next(mergeContactWrapper) //
                .next(validateContactBatchStore);
        if (useChangeList) {
            builder = builder //
                    .next(removeOrphanContact) //
                    .next(calcContactStats);
        } else {
            builder = builder //
                    .next(updateContactWorkflow) //
                    .next(rebuildContactWorkflow);
        }
        builder = builder.next(resetContact);
        return builder.build();
    }
}
