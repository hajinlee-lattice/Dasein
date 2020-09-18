package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.maintenance.SoftDeleteActivityStreamWrapper;
import com.latticeengines.cdl.workflow.steps.merge.BuildRawActivityStreamWrapper;
import com.latticeengines.cdl.workflow.steps.merge.MigrateActivityPartitionKey;
import com.latticeengines.cdl.workflow.steps.merge.PrepareForActivityStream;
import com.latticeengines.cdl.workflow.steps.process.AggActivityStreamToDaily;
import com.latticeengines.cdl.workflow.steps.process.FinishActivityStreamProcessing;
import com.latticeengines.cdl.workflow.steps.process.GenerateActivityAlert;
import com.latticeengines.cdl.workflow.steps.process.GenerateDimensionMetadata;
import com.latticeengines.cdl.workflow.steps.process.GenerateJourneyStage;
import com.latticeengines.cdl.workflow.steps.process.GenerateLastActivityDate;
import com.latticeengines.cdl.workflow.steps.process.GenerateTimeLine;
import com.latticeengines.cdl.workflow.steps.process.MergeActivityMetricsToEntityStep;
import com.latticeengines.cdl.workflow.steps.process.MetricsGroupsGenerationStep;
import com.latticeengines.cdl.workflow.steps.process.PeriodStoresGenerationStep;
import com.latticeengines.cdl.workflow.steps.process.ProfileAccountActivityMetricsWrapper;
import com.latticeengines.cdl.workflow.steps.process.ProfileContactActivityMetricsWrapper;
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
    private MigrateActivityPartitionKey migrateActivityPartitionKey;

    @Inject
    private SoftDeleteActivityStreamWrapper softDeleteActivityStreamWrapper;

    @Inject
    private BuildRawActivityStreamWrapper buildRawActivityStreamWrapper;

    @Inject
    private GenerateDimensionMetadata generateDimensionMetadata;

    @Inject
    private AggActivityStreamToDaily aggActivityStreamToDaily;

    @Inject
    private PeriodStoresGenerationStep periodStoresGenerationStep;

    @Inject
    private GenerateLastActivityDate generateLastActivityDate;

    @Inject
    private MetricsGroupsGenerationStep metricsGroupsGenerationStep;

    @Inject
    private MergeActivityMetricsToEntityStep mergeActivityMetricsToEntityStep;

    @Inject
    private ProfileAccountActivityMetricsWrapper profileAccountActivityMetricsWrapper;

    @Inject
    private ProfileContactActivityMetricsWrapper profileContactActivityMetricsWrapper;

    @Inject
    private GenerateTimeLine generateTimeLine;

    @Inject
    private GenerateJourneyStage generateJourneyStage;

    @Inject
    private GenerateActivityAlert generateActivityAlert;

    @Inject
    private FinishActivityStreamProcessing finishActivityStreamProcessing;

    @Override
    public Workflow defineWorkflow(ProcessActivityStreamWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(prepareForActivityStream) //
                .next(migrateActivityPartitionKey) // FIXME: remove after all tenants' activity stores migrated
                .next(softDeleteActivityStreamWrapper) //
                .next(buildRawActivityStreamWrapper) //
                .next(generateDimensionMetadata) //
                .next(aggActivityStreamToDaily) //
                .next(periodStoresGenerationStep) //
                .next(generateLastActivityDate) //
                .next(metricsGroupsGenerationStep) //
                .next(mergeActivityMetricsToEntityStep) //
                .next(profileAccountActivityMetricsWrapper) //
                .next(profileContactActivityMetricsWrapper) //
                .next(generateTimeLine) //
                .next(generateJourneyStage) //
                .next(generateActivityAlert) //
                .next(finishActivityStreamProcessing) //
                .build();
    }
}
