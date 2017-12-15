package com.latticeengines.cdl.workflow.choreographers;

import static com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep.RATING_MODELS;
import static com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep.TABLE_GOING_TO_REDSHIFT;

import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.RedshiftPublishWorkflow;
import com.latticeengines.cdl.workflow.steps.rating.IngestRatingFromRedshift;
import com.latticeengines.cdl.workflow.steps.rating.PrepareForRating;
import com.latticeengines.domain.exposed.pls.RatingModelContainer;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.BaseProcessEntityStepConfiguration;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractStep;
import com.latticeengines.workflow.exposed.build.BaseChoreographer;
import com.latticeengines.workflow.exposed.build.Choreographer;

@Component("processRatingChoreographer")
public class ProcessRatingChoreographer extends BaseChoreographer implements Choreographer {

    @Inject
    private PrepareForRating prepareForRating;

    @Inject
    private IngestRatingFromRedshift ingestRatingFromRedshift;

    @Inject
    private RedshiftPublishWorkflow redshiftPublishWorkflow;

    private boolean initialized;
    private boolean enforceRebuild = false;
    private boolean hasModels = false;

    @Override
    public boolean skipStep(AbstractStep<? extends BaseStepConfiguration> step, int seq) {
        if (isPrepareStep(step)) {
            checkEnforcedRebuild(step);
            return false;
        }
        if (isIngestStep(step)) {
            initialize(step);
        }
        if (inPublishWorkflow(seq)) {
            return skipPublishWorkflow(step);
        }

        return !enforceRebuild;

        // TODO: resume following after rating workflow is complete
        // return !enforceRebuild || !hasModels;
    }

    private void initialize(AbstractStep<? extends BaseStepConfiguration> step) {
        if (!initialized) {
            hasModels = CollectionUtils.isNotEmpty(step.getListObjectFromContext(RATING_MODELS, RatingModelContainer.class));
            initialized = true;
        }
    }

    private boolean isPrepareStep(AbstractStep<? extends BaseStepConfiguration> step) {
        return step.name().equals(prepareForRating.name());
    }

    private boolean isIngestStep(AbstractStep<? extends BaseStepConfiguration> step) {
        return step.name().equals(ingestRatingFromRedshift.name());
    }

    private boolean inPublishWorkflow(int seq) {
        String namespace = getStepNamespace(seq);
        return namespace.contains("." + redshiftPublishWorkflow.name());
    }

    private boolean skipPublishWorkflow(AbstractStep<? extends BaseStepConfiguration> step) {
        Map<BusinessEntity, String> entityTableNames = step.getMapObjectFromContext(TABLE_GOING_TO_REDSHIFT,
                BusinessEntity.class, String.class);
        return MapUtils.isEmpty(entityTableNames);
    }

    private void checkEnforcedRebuild(AbstractStep<? extends BaseStepConfiguration> step) {
        BaseProcessEntityStepConfiguration configuration = (BaseProcessEntityStepConfiguration) step.getConfiguration();
        enforceRebuild = Boolean.TRUE.equals(configuration.getRebuild());
    }

}
