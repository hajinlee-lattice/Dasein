package com.latticeengines.cdl.workflow.choreographers;

import static com.latticeengines.workflow.exposed.build.BaseWorkflowStep.CDL_ACTIVE_VERSION;
import static com.latticeengines.workflow.exposed.build.BaseWorkflowStep.CHOREOGRAPHER_CONTEXT_KEY;
import static com.latticeengines.workflow.exposed.build.BaseWorkflowStep.CUSTOMER_SPACE;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
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
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.util.PAReportUtils;
import com.latticeengines.domain.exposed.util.PeriodStrategyUtils;
import com.latticeengines.domain.exposed.util.ProductUtils;
import com.latticeengines.domain.exposed.util.TransactionUtils;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
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
    private boolean isBusinessCalenderChanged = false;

    @Override
    void checkManyUpdate(AbstractStep<? extends BaseStepConfiguration> step) {
        hasManyUpdate = false;
    }

    @Override
    protected void doInitialize(AbstractStep<? extends BaseStepConfiguration> step) {
        super.doInitialize(step);
        checkBusinessCalendarChanged(step);
    }

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
        hasAccounts = checkHasAccounts(step);
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
    protected AbstractStep<?> mergeStep() {
        return mergeTransaction;
    }

    @Override
    protected AbstractStep<?> cloneStep() {
        return cloneTransaction;
    }

    @Override
    protected AbstractStep<?> resetStep() {
        return resetTransaction;
    }

    @Override
    protected AbstractWorkflow<?> updateWorkflow() {
        return updateTransactionWorkflow;
    }

    @Override
    protected AbstractWorkflow<?> rebuildWorkflow() {
        return rebuildTransactionWorkflow;
    }

    @Override
    protected BusinessEntity mainEntity() {
        return BusinessEntity.Transaction;
    }

    @Override
    protected boolean shouldReset(AbstractStep<? extends BaseStepConfiguration> step) {
        if (!hasRawStore && !shouldMerge(step)) {
            log.info("No raw store and no imports, going to reset entity.");
            return true;
        }
        return false;
    }

    @Override
    protected boolean shouldRebuild() {
        boolean should = super.shouldRebuild();

        log.info(String.format(
                "Important flag to decide transaction rebuild: reset=%b, hasRawStore=%b, "
                        + "hasProducts=%b, productChoreographer.hasChange=%b, isBusinessCalendarChanged=%b",
                reset, hasRawStore, hasProducts, productChoreographer.hasChange, isBusinessCalenderChanged));

        if (reset) {
            return should;
        }

        if (!should) {
            if (hasRawStore && hasProducts && productChoreographer.hasChange) {
                log.info("Need to rebuild " + mainEntity() + " due to Product changes.");
                should = true;
            } else if (isBusinessCalenderChanged) {
                log.info("Need to rebuild " + mainEntity() + " due to business calendar changed.");
                should = true;
            }
        } else if (!hasProducts) {
            log.info("Skip rebuild " + mainEntity() + " due to missing product table.");
            should = false;
        }
        return should;
    }

    @Override
    protected Set<String> getExtraDecisions() {
        TreeSet<String> decisions = new TreeSet<>();
        decisions.add(isBusinessCalenderChanged ? "isBusinessCalenderChanged=true" : "");
        decisions.add(hasRawStore && hasProducts && productChoreographer.hasChange ? "hasProductChange=true" : "");
        return decisions;
    }

    @Override
    protected boolean shouldUpdate() {
        boolean should = super.shouldUpdate();

        log.info(String.format("Important flag to decide transaction update: hasProducts=%b", hasProducts));

        if (should && !hasProducts) {
            log.info("Skip update " + mainEntity() + " due to missing product table.");
            should = false;
        }
        return should;
    }

    private boolean isProfilePurchaseHistory(AbstractStep<? extends BaseStepConfiguration> step) {
        return step.name().contains(ProfilePurchaseHistory.BEAN_NAME);
    }

    void checkBusinessCalendarChanged(AbstractStep<? extends BaseStepConfiguration> step) {
        ChoreographerContext context = step.getObjectFromContext(CHOREOGRAPHER_CONTEXT_KEY, ChoreographerContext.class);
        isBusinessCalenderChanged = context.isBusinessCalenderChanged();
    }

    private boolean shouldCalculatePurchaseHistory(AbstractStep<? extends BaseStepConfiguration> step, int seq) {
        boolean shouldCalc = false;

        ChoreographerContext grapherContext = step.getObjectFromContext(CHOREOGRAPHER_CONTEXT_KEY,
                ChoreographerContext.class);
        boolean purchaseMetricsChanged = grapherContext.isPurchaseMetricsChanged();

        log.info(String.format(
                "Important flag to decide purchase history profile: purchaseMetricsChanged=%b, hasProducts=%b, hasAccounts=%b, hasRawStore=%b, update=%b, rebuild=%b, accountChoreographer.update=%b, accountChoreographer.rebuildNotForDataCloudChange=%b, productChoreographer.hasChange=%b",
                purchaseMetricsChanged, hasProducts, hasAccounts, hasRawStore, update, rebuild,
                accountChoreographer.update, accountChoreographer.rebuildNotForDataCloudChange,
                productChoreographer.hasChange));

        if (hasProducts && hasAccounts) {
            if (hasRawStore && (accountChoreographer.update || (accountChoreographer.rebuildNotForDataCloudChange))) {
                log.info("Need to rebuild purchase history due to Account changes.");
                shouldCalc = true;
            }
            if (hasRawStore && productChoreographer.hasChange) {
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
        boolean foundAnalyticProduct = ProductUtils.hasAnalyticProduct(yarnConfiguration, productTable);
        if (!foundAnalyticProduct) {
            log.info("Didn't find Analytic Product in " + productTable.getName());
            String warning = "No analytic product found. Skip generating curated attributes.";
            addWarningToProductReport(step, warning);
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
        } else {
            log.info("Did not find Analytic Product in table " + yearTable.getName());
            String warning = "No analytic product id matched between products and transactions. Skip generating curated attributes.";
            addWarningToProductReport(step, warning);
        }
        return false;
    }

    private void addWarningToProductReport(AbstractStep<? extends BaseStepConfiguration> step, String warning) {
        ObjectNode jsonReport = step.getObjectFromContext(ReportPurpose.PROCESS_ANALYZE_RECORDS_SUMMARY.getKey(),
                ObjectNode.class);
        jsonReport = PAReportUtils.appendMessageToProductReport(jsonReport, warning, true);
        step.putObjectInContext(ReportPurpose.PROCESS_ANALYZE_RECORDS_SUMMARY.getKey(), jsonReport);
    }

    @VisibleForTesting
    void setProductChoreographer(ProcessProductChoreographer productChoreographer) {
        this.productChoreographer = productChoreographer;
    }

    @VisibleForTesting
    void setHasRawStore(boolean hasRawStore) {
        this.hasRawStore = hasRawStore;
    }

    @VisibleForTesting
    void setHasProducts(boolean hasProducts) {
        this.hasProducts = hasProducts;
    }

    @VisibleForTesting
    void setHasProductChange(boolean hasProductChange) {
        this.productChoreographer.hasChange = hasProductChange;
    }

    @Override
    protected boolean skipsStepInSubWorkflow(AbstractStep<? extends BaseStepConfiguration> step, int seq) {
        return false;
    }
}
