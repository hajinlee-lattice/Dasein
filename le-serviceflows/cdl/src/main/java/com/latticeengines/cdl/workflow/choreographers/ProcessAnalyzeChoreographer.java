package com.latticeengines.cdl.workflow.choreographers;

import static com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep.SERVING_STORE_IN_STATS;
import static com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep.TABLE_GOING_TO_REDSHIFT;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.ProcessAccountWorkflow;
import com.latticeengines.cdl.workflow.ProcessContactWorkflow;
import com.latticeengines.cdl.workflow.ProcessProductWorkflow;
import com.latticeengines.cdl.workflow.ProcessRatingWorkflow;
import com.latticeengines.cdl.workflow.ProcessTransactionWorkflow;
import com.latticeengines.cdl.workflow.RedshiftPublishWorkflow;
import com.latticeengines.cdl.workflow.steps.process.AwsApsGeneratorStep;
import com.latticeengines.cdl.workflow.steps.process.CloneStatistics;
import com.latticeengines.cdl.workflow.steps.process.CombineStatistics;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractStep;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.BaseChoreographer;
import com.latticeengines.workflow.exposed.build.Choreographer;

@Component
public class ProcessAnalyzeChoreographer extends BaseChoreographer implements Choreographer {

    private static final Logger log = LoggerFactory.getLogger(ProcessAnalyzeChoreographer.class);

    @Inject
    private ProcessAccountChoreographer accountChoreographer;

    @Inject
    private ProcessContactChoreographer contactChoreographer;

    @Inject
    private ProcessProductChoreographer productChoreographer;

    @Inject
    private ProcessTransactionChoreographer transactionChoreographer;

    @Inject
    private ProcessRatingChoreographer ratingChoreographer;

    @Inject
    private ProcessAccountWorkflow accountWorkflow;

    @Inject
    private ProcessContactWorkflow contactWorkflow;

    @Inject
    private ProcessProductWorkflow productWorkflow;

    @Inject
    private ProcessTransactionWorkflow transactionWorkflow;

    @Inject
    private ProcessRatingWorkflow ratingWorkflow;

    @Inject
    private AwsApsGeneratorStep awsApsGeneratorStep;

    @Inject
    private CombineStatistics combineStatistics;

    @Inject
    private CloneStatistics cloneStatistics;

    @Inject
    private RedshiftPublishWorkflow redshiftPublishWorkflow;

    @Override
    public boolean skipStep(AbstractStep<? extends BaseStepConfiguration> step, int seq) {
        boolean skip = false;
        if (isAccountStep(seq)) {
            skip = accountChoreographer.skipStep(step, seq);
        } else if (isContactStep(seq)) {
            skip = contactChoreographer.skipStep(step, seq);
        } else if (isProductStep(seq)) {
            skip = productChoreographer.skipStep(step, seq);
        } else if (isTransactionStep(seq)) {
            skip = transactionChoreographer.skipStep(step, seq);
        } else if (isApsGenerationStep(step)) {
            skip = skipApsGeneration();
        } else if (isCombineStatsStep(step)) {
            skip = skipCombineStatsStep();
        } else if (isCloneStatsStep(step)) {
            skip = skipCloneStatsStep();
        } else if (inPublishWorkflow(seq)) {
            skip = skipPublishWorkflow(step);
        } else if (isRatingStep(seq)) {
            skip = ratingChoreographer.skipStep(step, seq);
        }
        return super.skipStep(step, seq) || skip;
    }

    @Override
    public void linkStepNamespaces(List<String> stepNamespaces) {
        super.linkStepNamespaces(stepNamespaces);
        accountChoreographer.linkStepNamespaces(stepNamespaces);
        contactChoreographer.linkStepNamespaces(stepNamespaces);
        productChoreographer.linkStepNamespaces(stepNamespaces);
        transactionChoreographer.linkStepNamespaces(stepNamespaces);
        ratingChoreographer.linkStepNamespaces(stepNamespaces);
    }

    private boolean isAccountStep(int seq) {
        return inWorkflow(seq, accountWorkflow);
    }

    private boolean isContactStep(int seq) {
        return inWorkflow(seq, contactWorkflow);
    }

    private boolean isProductStep(int seq) {
        return inWorkflow(seq, productWorkflow);
    }

    private boolean isTransactionStep(int seq) {
        return inWorkflow(seq, transactionWorkflow);
    }

    private boolean isRatingStep(int seq) {
        return inWorkflow(seq, ratingWorkflow);
    }

    private boolean inWorkflow(int seq, AbstractWorkflow workflow) {
        String namespace = getStepNamespace(seq);
        return namespace.startsWith(workflow.name());
    }

    private boolean isCombineStatsStep(AbstractStep<? extends BaseStepConfiguration> step) {
        return step.name().equals(combineStatistics.name());
    }

    private boolean isCloneStatsStep(AbstractStep<? extends BaseStepConfiguration> step) {
        return step.name().equals(cloneStatistics.name());
    }

    private boolean skipCombineStatsStep() {
        Map<BusinessEntity, String> entityTableNames = combineStatistics.getMapObjectFromContext( //
                SERVING_STORE_IN_STATS, BusinessEntity.class, String.class);
        if (MapUtils.isEmpty(entityTableNames)) {
            log.info("Skip combine stats step because there is no serving stores in stats");
            return true;
        } else {
            return false;
        }
    }

    private boolean skipCloneStatsStep() {
        return !skipCombineStatsStep();
    }

    private boolean inPublishWorkflow(int seq) {
        String namespace = getStepNamespace(seq);
        return namespace.startsWith(redshiftPublishWorkflow.name());
    }

    private boolean skipPublishWorkflow(AbstractStep<? extends BaseStepConfiguration> step) {
        Map<BusinessEntity, String> entityTableNames = step.getMapObjectFromContext(TABLE_GOING_TO_REDSHIFT,
                BusinessEntity.class, String.class);
        return MapUtils.isEmpty(entityTableNames);
    }

    private boolean isApsGenerationStep(AbstractStep<? extends BaseStepConfiguration> step) {
        return step.name().equals(awsApsGeneratorStep.name());
    }

    private boolean skipApsGeneration() {
        boolean skip = false;
        if (!transactionChoreographer.update && !transactionChoreographer.rebuild) {
            log.info("Skip APS generation because there is no change in Transaction data.");
            skip = true;
        }
        return skip;
    }


}
