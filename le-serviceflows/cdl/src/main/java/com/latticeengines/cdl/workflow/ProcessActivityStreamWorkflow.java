package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.merge.BuildRawActivityStreamWrapper;
import com.latticeengines.cdl.workflow.steps.merge.PrepareForActivityStream;
import com.latticeengines.cdl.workflow.steps.process.AggActivityStreamToDaily;
import com.latticeengines.cdl.workflow.steps.process.GenerateDimensionMetadata;
import com.latticeengines.cdl.workflow.steps.process.PeriodStoresGenerationStep;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.ProcessActivityStreamWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component(ProcessActivityStreamWorkflow.WORKFLOW_NAME)
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ProcessActivityStreamWorkflow extends AbstractWorkflow<ProcessActivityStreamWorkflowConfiguration> {

    static final String WORKFLOW_NAME = "processActivityStreamWorkflow";

    @Inject
    private PrepareForActivityStream prepareForActivityStream;

    @Inject
    private BuildRawActivityStreamWrapper buildRawActivityStreamWrapper;

    @Inject
    private GenerateDimensionMetadata generateDimensionMetadata;

    @Inject
    private AggActivityStreamToDaily aggActivityStreamToDaily;

    @Inject
    private PeriodStoresGenerationStep periodStoresGenerationStep;

    @Override
    public Workflow defineWorkflow(ProcessActivityStreamWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(prepareForActivityStream) //
                .next(buildRawActivityStreamWrapper) //
                .next(generateDimensionMetadata) //
                .next(aggActivityStreamToDaily) //
                .next(periodStoresGenerationStep) //
                .build();
    }
}
