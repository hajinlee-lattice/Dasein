package com.latticeengines.cdl.workflow.choreographers;

import static com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep.CDL_ACTIVE_VERSION;
import static com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep.CUSTOMER_SPACE;

import java.util.Arrays;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.RebuildTransactionWorkflow;
import com.latticeengines.cdl.workflow.UpdateTransactionWorkflow;
import com.latticeengines.cdl.workflow.steps.merge.MergeTransaction;
import com.latticeengines.cdl.workflow.steps.rebuild.ProfilePurchaseHistoryWrapper;
import com.latticeengines.cdl.workflow.steps.reset.ResetTransaction;
import com.latticeengines.cdl.workflow.steps.update.CloneTransaction;
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
    private CloneTransaction cloneTransaction;

    @Inject
    private ResetTransaction resetTransaction;

    @Inject
    private UpdateTransactionWorkflow updateTransactionWorkflow;

    @Inject
    private RebuildTransactionWorkflow rebuildTransactionWorkflow;

    @Inject
    private ProcessAccountChoreographer accountChoreographer;

    @Inject
    private ProcessProductChoreographer productChoreographer;

    @Inject
    private ProfilePurchaseHistoryWrapper profilePurchaseHistoryWrapper;

    private boolean hasRawStore = false;
    private boolean hasProducts = false;
    private boolean hasAccounts = false;

    @Override
    protected void checkActiveServingStore(AbstractStep<? extends BaseStepConfiguration> step) {
        DataCollection.Version active = step.getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        String customerSpace = step.getStringValueFromContext(CUSTOMER_SPACE);
        hasActiveServingStore = true;
        for (TableRoleInCollection servingStore: Arrays.asList( //
                BusinessEntity.Transaction.getServingStore(), //
                BusinessEntity.PeriodTransaction.getServingStore() //
        )) {
            String tableName = dataCollectionProxy.getTableName(customerSpace, servingStore, active);
            boolean hasServingStore = StringUtils.isNotBlank(tableName);
            if (hasServingStore) {
                log.info("Found " + servingStore + " in active version.");
            } else {
                log.info("No active " + servingStore);
            }
            hasActiveServingStore = hasActiveServingStore && hasServingStore;
        }
        if (hasActiveServingStore) {
            log.info("Found all serving stores in active version.");
        } else {
            log.info("Not all serving stores exists in active version.");
        }
        checkActiveRawStores(step);
        checkHasProducts(step);
        checkHasAccounts(step);
        hasActiveServingStore = hasActiveServingStore && hasRawStore && hasProducts;
    }

    private void checkActiveRawStores(AbstractStep<? extends BaseStepConfiguration> step) {
        DataCollection.Version active = step.getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        String customerSpace = step.getObjectFromContext(CUSTOMER_SPACE, String.class);
        String rawTableName = dataCollectionProxy.getTableName(customerSpace, //
                TableRoleInCollection.ConsolidatedRawTransaction, active);
        hasRawStore = StringUtils.isNotBlank(rawTableName);
        if (hasRawStore) {
            log.info("Found raw period store.");
        } else {
            log.info("No raw period store");
        }
    }

    private void checkHasProducts(AbstractStep<? extends BaseStepConfiguration> step) {
        DataCollection.Version active = step.getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        String customerSpace = step.getObjectFromContext(CUSTOMER_SPACE, String.class);
        String rawTableName = dataCollectionProxy.getTableName(customerSpace, //
                TableRoleInCollection.ConsolidatedProduct, active.complement());
        if (StringUtils.isBlank(rawTableName)) {
            rawTableName = dataCollectionProxy.getTableName(customerSpace, //
                    TableRoleInCollection.ConsolidatedProduct, active);
        }
        hasProducts = StringUtils.isNotBlank(rawTableName);
        if (hasProducts) {
            log.info("Found product batch store.");
        } else {
            log.info("No product batch store.");
        }
    }

    private void checkHasAccounts(AbstractStep<? extends BaseStepConfiguration> step) {
        DataCollection.Version active = step.getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        String customerSpace = step.getObjectFromContext(CUSTOMER_SPACE, String.class);
        String rawTableName = dataCollectionProxy.getTableName(customerSpace, //
                TableRoleInCollection.ConsolidatedAccount, active.complement());
        if (StringUtils.isBlank(rawTableName)) {
            rawTableName = dataCollectionProxy.getTableName(customerSpace, //
                    TableRoleInCollection.ConsolidatedAccount, active);
        }
        hasAccounts = StringUtils.isNotBlank(rawTableName);
        if (hasAccounts) {
            log.info("Found account batch store.");
        } else {
            log.info("Noaccount batch store.");
        }
    }

    @Override
    public boolean skipStep(AbstractStep<? extends BaseStepConfiguration> step, int seq) {

        boolean skip = isCommonSkip(step, seq);
        if (isProfilePurchaseHistory(seq)) {
            if (skip) {
                skip = !shouldCalculatePurchaseHistory();
            } else {
                log.info("Skip rebuild purchase history due to Missing account.");
                skip = !hasAccounts;
            }
        }
        return skip;
    }

    @Override
    protected AbstractStep mergeStep() {
        return mergeTransaction;
    }

    @Override
    protected AbstractStep cloneStep() {
        return cloneTransaction;
    }

    @Override
    protected AbstractStep resetStep() {
        return resetTransaction;
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

    @Override
    protected boolean shouldRebuild() {
        boolean should = super.shouldRebuild();
        if (!should) {
            if (hasRawStore && hasProducts) {
                if (productChoreographer.update || productChoreographer.rebuild) {
                    log.info("Need to rebuild " + mainEntity() + " due to Product changes.");
                    should = true;
                }
            }
        } else if (!hasProducts) {
            log.info("Skip rebuild " + mainEntity() + " due to missing product table.");
            should = false;
        }
        return should;
    }

    @Override
    protected boolean shouldUpdate() {
        boolean should = super.shouldUpdate();
        if (should && !hasProducts) {
            log.info("Skip update " + mainEntity() + " due to missing product table.");
            should = false;
        }
        return should;
    }

    private boolean isProfilePurchaseHistory(int seq) {
        String namespace = getStepNamespace(seq);
        return namespace.contains("." + profilePurchaseHistoryWrapper.name());
    }

    private boolean shouldCalculatePurchaseHistory() {
        if (hasRawStore && hasProducts && hasAccounts) {
            if (accountChoreographer.update || accountChoreographer.rebuild) {
                log.info("Need to rebuild purchase history due to Account changes.");
                return true;
            }
            if (update) {
                log.info("Need to rebuild purchase history due to Transaction changes.");
                return true;
            }
        }
        return false;
    }

}
