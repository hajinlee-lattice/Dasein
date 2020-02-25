package com.latticeengines.cdl.workflow.choreographers;

import static com.latticeengines.workflow.exposed.build.BaseWorkflowStep.HARD_DEELETE_ACTIONS;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.rematch.DeleteByUploadStep;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractStep;
import com.latticeengines.workflow.exposed.build.BaseChoreographer;

@Component
public class ConvertBatchStoreToDataTableChoreographer extends BaseChoreographer {

    @Inject
    private DeleteByUploadStep deleteByUploadStep;

    private static final Logger log = LoggerFactory.getLogger(ConvertBatchStoreToDataTableChoreographer.class);

    @Override
    public boolean skipStep(AbstractStep<? extends BaseStepConfiguration> step, int seq) {
        if (isHardDeleteByUploadStep(step)) {
            return skipHardDeleteByUpload(step);
        } else {
            return false;
        }
    }

    private boolean isHardDeleteByUploadStep(AbstractStep<? extends BaseStepConfiguration> step) {
        return step.name().endsWith(deleteByUploadStep.name());
    }

    private boolean skipHardDeleteByUpload(AbstractStep<? extends BaseStepConfiguration> step) {
        List<Action> actions = step.getListObjectFromContext(HARD_DEELETE_ACTIONS, Action.class);
        return CollectionUtils.isEmpty(actions);
    }
}
