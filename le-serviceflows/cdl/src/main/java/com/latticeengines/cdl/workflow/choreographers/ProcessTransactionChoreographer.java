package com.latticeengines.cdl.workflow.choreographers;

import static com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep.CDL_ACTIVE_VERSION;
import static com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep.CUSTOMER_SPACE;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.RebuildTransactionWorkflow;
import com.latticeengines.cdl.workflow.UpdateTransactionWorkflow;
import com.latticeengines.cdl.workflow.steps.merge.MergeTransaction;
import com.latticeengines.cdl.workflow.steps.update.ClonePurchaseHistory;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractStep;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Choreographer;

@Component
public class ProcessTransactionChoreographer extends AbstractProcessEntityChoreographer implements Choreographer {

    private static final Logger log = LoggerFactory.getLogger(ProcessTransactionChoreographer.class);

    @Inject
    private MergeTransaction mergeTransaction;

    @Inject
    private ClonePurchaseHistory clonePurchaseHistory;

    @Inject
    private UpdateTransactionWorkflow updateTransactionWorkflow;

    @Inject
    private RebuildTransactionWorkflow rebuildTransactionWorkflow;

    private boolean hasActivePeriodStores = false;

    @Override
    protected void checkActiveServingStore(AbstractStep<? extends BaseStepConfiguration> step) {
        TableRoleInCollection servingStore = TableRoleInCollection.CalculatedPurchaseHistory;
        DataCollection.Version active = step.getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        String customerSpace = step.getObjectFromContext(CUSTOMER_SPACE, String.class);
        String tableName = dataCollectionProxy.getTableName(customerSpace, servingStore, active);
        hasActiveServingStore = StringUtils.isNotBlank(tableName);
        if (hasActiveServingStore) {
            log.info("Found " + servingStore + " in active version.");
        } else {
            log.info("No active " + servingStore);
        }
        checkActivePeriodStores(step);
    }

    @Override
    protected boolean shouldRebuild() {
        if (hasImports && !(hasActiveServingStore && hasActivePeriodStores)) {
            log.info("Has imports but no serving or period stores, going to rebuild " + mainEntity());
            return false;
        }
        log.info("No reason to rebuild " + mainEntity());
        return false;
    }

    @Override
    protected boolean shouldUpdate() {
        if (hasImports && hasActiveServingStore && hasActivePeriodStores) {
            log.info("Has imports but no schema change, going to update " + mainEntity());
            return true;
        }
        log.info("No reason to update " + mainEntity());
        return false;
    }

    private void checkActivePeriodStores(AbstractStep<? extends BaseStepConfiguration> step) {
        DataCollection.Version active = step.getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        String customerSpace = step.getObjectFromContext(CUSTOMER_SPACE, String.class);
        String rawTableName = dataCollectionProxy.getTableName(customerSpace, TableRoleInCollection.ConsolidatedRawTransaction, active);
        String dailyTableName = dataCollectionProxy.getTableName(customerSpace, TableRoleInCollection.ConsolidatedDailyTransaction, active);
        String periodTableName = dataCollectionProxy.getTableName(customerSpace, TableRoleInCollection.ConsolidatedPeriodTransaction, active);

        hasActivePeriodStores = StringUtils.isNotBlank(rawTableName)
                && StringUtils.isNotBlank(dailyTableName)
                && StringUtils.isNotBlank(periodTableName);

        if (hasActivePeriodStores) {
            log.info("Found period stores.");
        } else {
            log.info("No active period stores");
        }
    }

    @Override
    public boolean skipStep(AbstractStep<? extends BaseStepConfiguration> step, int seq) {
        return isCommonSkip(step, seq);
    }

    @Override
    protected AbstractStep mergeStep() {
        return mergeTransaction;
    }

    @Override
    protected AbstractStep cloneStep() {
        return clonePurchaseHistory;
    }

    @Override
    protected AbstractWorkflow updateWorkflow() {
        return updateTransactionWorkflow;
    }

    @Override
    protected AbstractWorkflow rebuildWorkflow() {
        return rebuildTransactionWorkflow;
    }

    @Override
    protected BusinessEntity mainEntity() {
        return BusinessEntity.Transaction;
    }

}
