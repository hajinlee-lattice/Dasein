package com.latticeengines.cdl.workflow.choreographers;

import static com.latticeengines.workflow.exposed.build.BaseWorkflowStep.CDL_ACTIVE_VERSION;
import static com.latticeengines.workflow.exposed.build.BaseWorkflowStep.CHOREOGRAPHER_CONTEXT_KEY;
import static com.latticeengines.workflow.exposed.build.BaseWorkflowStep.CUSTOMER_SPACE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.RebuildTransactionWorkflow;
import com.latticeengines.cdl.workflow.UpdateTransactionWorkflow;
import com.latticeengines.cdl.workflow.steps.merge.MergeTransaction;
import com.latticeengines.cdl.workflow.steps.rebuild.ProfilePurchaseHistory;
import com.latticeengines.cdl.workflow.steps.reset.ResetTransaction;
import com.latticeengines.cdl.workflow.steps.update.CloneTransaction;
import com.latticeengines.domain.exposed.cdl.ChoreographerContext;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.transaction.Product;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.util.PeriodStrategyUtils;
import com.latticeengines.domain.exposed.util.ProductUtils;
import com.latticeengines.domain.exposed.util.TransactionUtils;
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
    private YarnConfiguration yarnConfiguration;

    private boolean hasRawStore = false;
    private boolean hasProducts = false;
    private boolean hasAccounts = false;

    @Override
    protected void checkActiveServingStore(AbstractStep<? extends BaseStepConfiguration> step) {
        DataCollection.Version active = step.getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        String customerSpace = step.getStringValueFromContext(CUSTOMER_SPACE);
        hasActiveServingStore = true;
        for (TableRoleInCollection servingStore : Arrays.asList( //
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

        boolean skip;
        if (isProfilePurchaseHistory(step)) {
            skip = !shouldCalculatePurchaseHistory(step, seq);
        } else {
            skip = isCommonSkip(step, seq);
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
    protected boolean shouldReset() {
        if (!hasRawStore && !shouldMerge()) {
            log.info("No raw store and no imports, going to reset entity.");
            return true;
        }
        return false;
    }

    @Override
    protected boolean shouldRebuild() {
        boolean should = super.shouldRebuild();

        if (reset) {
            return should;
        }

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

    private boolean isProfilePurchaseHistory(AbstractStep<? extends BaseStepConfiguration> step) {
        log.info(String.format(
                "Check whether it is profile purchase history: StepName=%s, ProfilePurchaseHistoryBean=%s", step.name(),
                ProfilePurchaseHistory.BEAN_NAME));
        return step.name().contains(ProfilePurchaseHistory.BEAN_NAME);
    }

    private boolean shouldCalculatePurchaseHistory(AbstractStep<? extends BaseStepConfiguration> step, int seq) {
        boolean shouldCalc = false;

        ChoreographerContext grapherContext = step.getObjectFromContext(CHOREOGRAPHER_CONTEXT_KEY,
                ChoreographerContext.class);
        boolean purchaseMetricsChanged = grapherContext.isPurchaseMetricsChanged();

        if (hasProducts && hasAccounts) {
            if (hasRawStore && (accountChoreographer.update || (accountChoreographer.commonRebuild))) {
                log.info("Need to rebuild purchase history due to Account changes.");
                shouldCalc = true;
            }
            if (hasRawStore && (productChoreographer.update || productChoreographer.rebuild)) {
                log.info("Need to rebuild purchase history due to Product changes.");
                shouldCalc = true;
            }
            if (hasRawStore && purchaseMetricsChanged) {
                log.info("Need to rebuild purchase history due to curated metrics configuration changes.");
                shouldCalc = true;
            }
            if (update || rebuild) {
                log.info("Need to rebuild purchase history due to Transaction changes.");
                shouldCalc = true;
            }
        }

        if (shouldCalc) {
            shouldCalc = hasAnalyticProduct(step);
        }
        return shouldCalc;
    }

    private boolean hasAnalyticProduct(AbstractStep<? extends BaseStepConfiguration> step) {
        DataCollection.Version active = step.getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        String customerSpace = step.getStringValueFromContext(CUSTOMER_SPACE);
        Table productTable = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.ConsolidatedProduct,
                active.complement());
        if (productTable == null) {
            log.info("Did not find product table in inactive version.");
            productTable = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.ConsolidatedProduct,
                    active);
            if (productTable == null) {
                throw new IllegalStateException("Cannot find the product table in both versions");
            }
        }

        log.info(String.format("productTableName for customer %s is %s", customerSpace, productTable.getName()));
        boolean foundAnalyticProduct = false;
        List<Product> productList = new ArrayList<>(
                ProductUtils.loadProducts(yarnConfiguration, productTable.getExtracts().get(0).getPath()));
        for (Product product : productList) {
            if (ProductType.Analytic.name().equals(product.getProductType())) {
                foundAnalyticProduct = true;
                break;
            }
        }
        if (!foundAnalyticProduct) {
            log.info("Didn't find Analytic Product in " + productTable.getName());
            return false;
        }

        List<Table> periodTables = dataCollectionProxy.getTables(customerSpace,
                TableRoleInCollection.ConsolidatedPeriodTransaction, active.complement());
        if (CollectionUtils.isEmpty(periodTables)) {
            log.info("Did not find period transaction table in inactive version.");
            periodTables = dataCollectionProxy.getTables(customerSpace,
                    TableRoleInCollection.ConsolidatedPeriodTransaction, active);
            if (CollectionUtils.isEmpty(periodTables)) {
                log.info(
                        "Did not find period transaction table in both versions. Treated as no transaction existing with Analytic Product");
                return false;
            }
        }
        Table yearTable = PeriodStrategyUtils.findPeriodTableFromStrategy(periodTables, PeriodStrategy.CalendarYear);

        log.info("Checking Analytic Product existence in table " + yearTable.getName() + ". Might take long time.");
        if (TransactionUtils.hasAnalyticProduct(yarnConfiguration, yearTable.getExtracts().get(0).getPath())) {
            log.info("Found Analytic Product in table " + yearTable.getName());
            return true;
        }
        return false;
    }
}
