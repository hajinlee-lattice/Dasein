package com.latticeengines.cdl.workflow.choreographers;

import static com.latticeengines.workflow.exposed.build.BaseWorkflowStep.CONSOLIDATE_INPUT_IMPORTS;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.merge.MatchAccount;
import com.latticeengines.cdl.workflow.steps.merge.MatchContact;
import com.latticeengines.cdl.workflow.steps.merge.MatchTransaction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractStep;
import com.latticeengines.workflow.exposed.build.BaseChoreographer;

@Component
public class ProcessMatchEntityChoreographer extends BaseChoreographer {

    private static final Logger log = LoggerFactory.getLogger(ProcessMatchEntityChoreographer.class);

    @Inject
    private MatchAccount matchAccount;

    @Inject
    private MatchContact matchContact;

    @Inject
    private MatchTransaction matchTransaction;

    @Override
    public boolean skipStep(AbstractStep<? extends BaseStepConfiguration> step, int seq) {
        if (step.name().endsWith(matchAccount.name()) && hasNoImports(step, BusinessEntity.Account)) {
            log.info("Skip matchAccount, because no imports for Account");
            return true;
        } else if (step.name().endsWith(matchContact.name()) && hasNoImports(step, BusinessEntity.Contact)) {
            log.info("Skip matchContact, because no imports for Contact");
            return true;
        } else if (step.name().endsWith(matchTransaction.name()) && hasNoImports(step, BusinessEntity.Transaction)) {
            log.info("Skip matchTransaction, because no imports for Transaction");
            return true;
        } else {
            return false;
        }
    }

    private boolean hasNoImports(AbstractStep<? extends BaseStepConfiguration> step, BusinessEntity entity) {
        @SuppressWarnings("rawtypes")
        Map<BusinessEntity, List> entityImportsMap = step.getMapObjectFromContext(CONSOLIDATE_INPUT_IMPORTS,
                BusinessEntity.class, List.class);
        return MapUtils.isEmpty(entityImportsMap) || !entityImportsMap.containsKey(entity);
    }

}
