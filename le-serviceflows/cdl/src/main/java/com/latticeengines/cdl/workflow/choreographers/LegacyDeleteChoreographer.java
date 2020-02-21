package com.latticeengines.cdl.workflow.choreographers;

import static com.latticeengines.workflow.exposed.build.BaseWorkflowStep.ACCOUNT_LEGACY_DELTE_BYUOLOAD_ACTIONS;
import static com.latticeengines.workflow.exposed.build.BaseWorkflowStep.CONTACT_LEGACY_DELTE_BYUOLOAD_ACTIONS;
import static com.latticeengines.workflow.exposed.build.BaseWorkflowStep.LEGACY_DELETE_BYDATERANGE_ACTIONS;
import static com.latticeengines.workflow.exposed.build.BaseWorkflowStep.TRANSACTION_LEGACY_DELTE_BYUOLOAD_ACTIONS;

import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.legacydelete.LegacyDeleteByDateRangeStep;
import com.latticeengines.cdl.workflow.steps.legacydelete.LegacyDeleteByUploadStep;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.legacydelete.LegacyDeleteByDateRangeStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.legacydelete.LegacyDeleteByUploadStepConfiguration;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractStep;
import com.latticeengines.workflow.exposed.build.BaseChoreographer;

@Component
public class LegacyDeleteChoreographer extends BaseChoreographer {

    @Inject
    private LegacyDeleteByUploadStep legacyDeleteByUploadStep;

    @Inject
    private LegacyDeleteByDateRangeStep legacyDeleteByDateRangeStep;

    @Override
    public boolean skipStep(AbstractStep<? extends BaseStepConfiguration> step, int seq) {
        if (isLegacyDeleteByDateRangeStep(step)) {
            return skipLegacyDeleteByDateRange(step);
        } else if (isLegacyDeleteByUploadStep(step)) {
            return skipLegacyDeleteByUpload(step);
        } else {
            return false;
        }
    }

    private boolean isLegacyDeleteByUploadStep(AbstractStep<? extends BaseStepConfiguration> step) {
        return step.name().endsWith(legacyDeleteByUploadStep.name());
    }

    private boolean isLegacyDeleteByDateRangeStep(AbstractStep<? extends BaseStepConfiguration> step) {
        return step.name().endsWith(legacyDeleteByDateRangeStep.name());
    }

    private boolean skipLegacyDeleteByUpload(AbstractStep<? extends BaseStepConfiguration> step) {
        if (!(step.getConfiguration() instanceof LegacyDeleteByUploadStepConfiguration)) {
            return true;
        }
        LegacyDeleteByUploadStepConfiguration configuration = (LegacyDeleteByUploadStepConfiguration) step.getConfiguration();
        if (BusinessEntity.Account.equals(configuration.getEntity())) {
            Set<Action> actionSet = step.getSetObjectFromContext(ACCOUNT_LEGACY_DELTE_BYUOLOAD_ACTIONS, Action.class);
            return CollectionUtils.isEmpty(actionSet);
        } else if (BusinessEntity.Contact.equals(configuration.getEntity())) {
            Set<Action> actionSet = step.getSetObjectFromContext(CONTACT_LEGACY_DELTE_BYUOLOAD_ACTIONS, Action.class);
            return CollectionUtils.isEmpty(actionSet);
        } else if (BusinessEntity.Transaction.equals(configuration.getEntity())) {
            Map<CleanupOperationType, Set> actionMap = step.getMapObjectFromContext(TRANSACTION_LEGACY_DELTE_BYUOLOAD_ACTIONS,
                    CleanupOperationType.class, Set.class);
            return actionMap == null || actionMap.isEmpty();
        }
        return false;
    }

    private boolean skipLegacyDeleteByDateRange(AbstractStep<? extends BaseStepConfiguration> step) {
        Map<BusinessEntity, Set> actionMap = step.getMapObjectFromContext(LEGACY_DELETE_BYDATERANGE_ACTIONS,
                BusinessEntity.class, Set.class);
        if (!(step.getConfiguration() instanceof LegacyDeleteByDateRangeStepConfiguration)) {
            return true;
        }
        LegacyDeleteByDateRangeStepConfiguration configuration = (LegacyDeleteByDateRangeStepConfiguration) step.getConfiguration();
        if (actionMap == null || !actionMap.containsKey(configuration.getEntity())) {
            return true;
        }
        return CollectionUtils.isEmpty(JsonUtils.convertSet(actionMap.get(configuration.getEntity()), Action.class));
    }
}
