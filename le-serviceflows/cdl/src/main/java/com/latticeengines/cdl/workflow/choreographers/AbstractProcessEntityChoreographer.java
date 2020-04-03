package com.latticeengines.cdl.workflow.choreographers;

import static com.latticeengines.workflow.exposed.build.BaseWorkflowStep.CDL_ACTIVE_VERSION;
import static com.latticeengines.workflow.exposed.build.BaseWorkflowStep.CHOREOGRAPHER_CONTEXT_KEY;
import static com.latticeengines.workflow.exposed.build.BaseWorkflowStep.CONSOLIDATE_INPUT_IMPORTS;
import static com.latticeengines.workflow.exposed.build.BaseWorkflowStep.CUSTOMER_SPACE;
import static com.latticeengines.workflow.exposed.build.BaseWorkflowStep.ENTITIES_WITH_SCHEMA_CHANGE;
import static com.latticeengines.workflow.exposed.build.BaseWorkflowStep.ENTITY_MATCH_ENABLED;
import static com.latticeengines.workflow.exposed.build.BaseWorkflowStep.FULL_REMATCH_PA;
import static com.latticeengines.workflow.exposed.build.BaseWorkflowStep.HARD_DEELETE_ACTIONS;
import static com.latticeengines.workflow.exposed.build.BaseWorkflowStep.PROCESS_ANALYTICS_DECISIONS_KEY;
import static com.latticeengines.workflow.exposed.build.BaseWorkflowStep.SOFT_DEELETE_ACTIONS;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.cdl.ChoreographerContext;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.BaseProcessEntityStepConfiguration;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.workflow.exposed.build.AbstractStep;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.BaseChoreographer;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

public abstract class AbstractProcessEntityChoreographer extends BaseChoreographer {

    private static final Logger log = LoggerFactory.getLogger(AbstractProcessEntityChoreographer.class);

    boolean enforceRebuild = false;
    boolean hasSchemaChange = false;
    boolean hasActiveServingStore = false;
    boolean hasImports = false;
    boolean hasManyUpdate = false;
    boolean rebuildDueToActions = false;
    private boolean initialized = false;
    private boolean hasBatchStore = false;
    private boolean hasSystemStore = false;
    private boolean hasSoftDelete = false;
    private boolean hasHardDelete = false;
    float diffRate = 0;
    long diffCount = 0;

    boolean rebuild = false;
    boolean update = false;
    boolean reset = false;
    boolean replace = false;

    @Inject
    protected DataCollectionProxy dataCollectionProxy;

    /**
     * Steps that can be skipped based on common entity processing pattern
     */
    boolean isCommonSkip(AbstractStep<? extends BaseStepConfiguration> step, int seq) {
        String msg = String.format("Skip step [%d] %s", seq, step.name());

        initialize(step);

        if (isSoftDeleteStep(step)) {
            return !shouldSoftDelete(step);
        }

        if (isMergeStep(step)) {
            return !shouldMerge(step);
        }

        if (isCloneStep(step)) {
            checkSchemaChange(step);
            checkManyUpdate(step);
            reset = shouldReset(step);
            rebuild = shouldRebuild(step);
            update = shouldUpdate(step);
            log.info("reset=" + reset + ", replace=" + replace + ", rebuild=" + rebuild + ", update=" + update + ", entity=" + mainEntity());
            saveDecisions(step);
            if (reset && (rebuild || update)) {
                throw new IllegalStateException("When reset, neither rebuild nor update can be true.");
            }
            if (rebuild && update) {
                throw new IllegalStateException("Rebuild and update cannot be both true");
            }
        }

        if (belongsToUpdate(seq)) {
            if (!update) {
                log.info(msg + ", because not in update mode.");
                return true;
            }
        }

        if (belongsToRebuild(seq)) {
            if (!rebuild) {
                log.info(msg + ", because not in rebuild mode.");
                return true;
            }
        }

        if (isResetStep(step)) {
            if (!reset) {
                log.info(msg + ", because not in reset mode.");
                return true;
            }
        }

        if (skipsStepInSubWorkflow(step, seq)) {
            log.info(msg + " is in skipped workflow");
            return true;

        }

        return false;
    }

    protected boolean hasTableInMapCtx(AbstractStep<? extends BaseStepConfiguration> step, String mapCtxKey) {
        if (step == null || StringUtils.isBlank(mapCtxKey) || !step.hasKeyInContext(mapCtxKey)) {
            return false;
        }

        Map<String, String> tables = step.getMapObjectFromContext(mapCtxKey, String.class, String.class);
        if (MapUtils.isEmpty(tables)) {
            return false;
        }

        return tables.values().stream().anyMatch(StringUtils::isNotBlank);
    }

    private void saveDecisions(AbstractStep<? extends BaseStepConfiguration> step) {
        TreeSet<String> decisions = new TreeSet<>();
        decisions.add(reset ? "reset=true" : (update ? "update=true" : "rebuild=true"));
        decisions.add(enforceRebuild ? "enforceRebuild=true" : "");
        decisions.add(replace ? "replace=true" : "");
        decisions.add(hasSchemaChange ? "hasSchemaChange=true" : "");
        decisions.add(hasImports ? "hasImports=true" : "");
        decisions.add(hasSoftDelete ? "hasSoftDelete=true" : "");
        decisions.add(hasHardDelete ? "hasHardDelete=true" : "");
        decisions.add(hasManyUpdate ? "hasManyUpdate=true" : "");
        decisions.add(hasManyUpdate ? String.format("diffRate=%f", diffRate) : "");
        decisions.add(rebuildDueToActions ? "rebuildDueToActions=true" : "");
        decisions.addAll(getExtraDecisions());
        decisions.remove("");
        StringBuilder builder = new StringBuilder();
        for (String decision : decisions) {
            builder.append(decision).append(";");
        }
        Map<String, String> map = step.getMapObjectFromContext(PROCESS_ANALYTICS_DECISIONS_KEY, String.class,
                String.class);
        if (map == null) {
            map = new HashMap<>();
        }
        if (builder.length() > 0) {
            map.put(mainEntity().name(), builder.toString());
            step.putObjectInContext(PROCESS_ANALYTICS_DECISIONS_KEY, map);
        }
    }

    protected Set<String> getExtraDecisions() {
        return Collections.emptySet();
    }

    private boolean isSoftDeleteStep(AbstractStep<? extends BaseStepConfiguration> step) {
        if (softDeleteStep() == null) {
            return false;
        } else {
            return step.name().endsWith(softDeleteStep().name());
        }
    }

    private boolean isMergeStep(AbstractStep<? extends BaseStepConfiguration> step) {
        return step.name().endsWith(mergeStep().name());
    }

    private boolean isCloneStep(AbstractStep<? extends BaseStepConfiguration> step) {
        return step.name().endsWith(cloneStep().name());
    }

    private boolean isResetStep(AbstractStep<? extends BaseStepConfiguration> step) {
        return step.name().endsWith(resetStep().name());
    }

    boolean belongsToUpdate(int seq) {
        String namespace = getStepNamespace(seq);
        return updateWorkflow() != null && namespace.contains(updateWorkflow().name());
    }

    boolean belongsToRebuild(int seq) {
        String namespace = getStepNamespace(seq);
        return rebuildWorkflow() != null && namespace.contains(rebuildWorkflow().name());
    }

    private void initialize(AbstractStep<? extends BaseStepConfiguration> step) {
        if (!initialized) {
            doInitialize(step);
            initialized = true;
        }
    }

    protected void doInitialize(AbstractStep<? extends BaseStepConfiguration> step) {
        checkEnforcedRebuild(step);
        checkImports(step);
        checkSnHDelete(step);
        checkActiveServingStore(step);
        checkHasBatchStore(step);
        checkHasSystemBatchStore(step);
        checkRebuildDueToActions(step);
        checkHasReplace(step);
    }

    void checkRebuildDueToActions(AbstractStep<? extends BaseStepConfiguration> step) {
        ChoreographerContext grapherContext = step.getObjectFromContext(CHOREOGRAPHER_CONTEXT_KEY,
                ChoreographerContext.class);
        if (CollectionUtils.isNotEmpty(grapherContext.getEntitiesRebuildDueToActions())
                && grapherContext.getEntitiesRebuildDueToActions().contains(mainEntity())) {
            rebuildDueToActions = true;
        }
        log.info("Job impacted=" + rebuildDueToActions + " for " + mainEntity());
    }

    private void checkEnforcedRebuild(AbstractStep<? extends BaseStepConfiguration> step) {
        BaseProcessEntityStepConfiguration configuration = (BaseProcessEntityStepConfiguration) step.getConfiguration();
        enforceRebuild = Boolean.TRUE.equals(configuration.getRebuild());
    }

    private void checkImports(AbstractStep<? extends BaseStepConfiguration> step) {
        @SuppressWarnings("rawtypes")
        Map<BusinessEntity, List> entityImportsMap = step.getMapObjectFromContext(CONSOLIDATE_INPUT_IMPORTS,
                BusinessEntity.class, List.class);
        hasImports = (MapUtils.isNotEmpty(entityImportsMap) && entityImportsMap.containsKey(mainEntity()))
                || hasEmbeddedEntity(step);
        if (hasImports) {
            log.info("Found imports for " + mainEntity().name());
        } else {
            log.info("Found no imports for " + mainEntity().name());
        }
    }

    private void checkSnHDelete(AbstractStep<? extends BaseStepConfiguration> step) {
        List<Action> softDeletes = step.getListObjectFromContext(SOFT_DEELETE_ACTIONS, Action.class);
        List<Action> hardDeletes = step.getListObjectFromContext(HARD_DEELETE_ACTIONS, Action.class);
        hasHardDelete = CollectionUtils.isNotEmpty(hardDeletes);
        if (hasHardDelete) {
            log.info(String.format("Found %d hard delete actions", hardDeletes.size()));
        }
        hasSoftDelete = CollectionUtils.isNotEmpty(softDeletes);
        if (hasSoftDelete) {
            log.info(String.format("Found %d soft delete actions", softDeletes.size()));
        }
    }

    boolean hasValidSoftDeleteActions(List<Action> softDeletes) {
        return CollectionUtils.isNotEmpty(softDeletes);
    }

    private void checkSchemaChange(AbstractStep<? extends BaseStepConfiguration> step) {
        List<BusinessEntity> entityList = step.getListObjectFromContext(ENTITIES_WITH_SCHEMA_CHANGE,
                BusinessEntity.class);
        if (CollectionUtils.isNotEmpty(entityList) && entityList.contains(mainEntity())) {
            hasSchemaChange = true;
        }
    }

    protected void checkActiveServingStore(AbstractStep<? extends BaseStepConfiguration> step) {
        TableRoleInCollection servingStore = mainEntity().getServingStore();
        if (servingStore != null) {
            DataCollection.Version active = step.getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
            String customerSpace = step.getObjectFromContext(CUSTOMER_SPACE, String.class);
            String tableName = dataCollectionProxy.getTableName(customerSpace, servingStore, active);
            hasActiveServingStore = StringUtils.isNotBlank(tableName);
        }
        if (hasActiveServingStore) {
            log.info("Found serving store for entity " + mainEntity());
        } else {
            log.info("No active serving store for entity " + mainEntity());
        }
    }

    private void checkHasBatchStore(AbstractStep<? extends BaseStepConfiguration> step) {
        TableRoleInCollection batchStore = mainEntity().getBatchStore();
        if (batchStore != null) {
            DataCollection.Version active = step.getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
            String customerSpace = step.getObjectFromContext(CUSTOMER_SPACE, String.class);
            String tableName = dataCollectionProxy.getTableName(customerSpace, batchStore, active.complement());
            if (StringUtils.isBlank(tableName)) {
                tableName = dataCollectionProxy.getTableName(customerSpace, batchStore, active);
            }
            hasBatchStore = StringUtils.isNotBlank(tableName);
        }
        if (hasBatchStore) {
            log.info("Found batch store for entity " + mainEntity());
        } else {
            log.info("No batch store for entity " + mainEntity());
        }
    }

    private void checkHasSystemBatchStore(AbstractStep<? extends BaseStepConfiguration> step) {
        TableRoleInCollection systemStore = mainEntity().getSystemBatchStore();
        if (systemStore != null) {
            DataCollection.Version active = step.getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
            String customerSpace = step.getObjectFromContext(CUSTOMER_SPACE, String.class);
            String tableName = dataCollectionProxy.getTableName(customerSpace, systemStore, active.complement());
            if (StringUtils.isBlank(tableName)) {
                tableName = dataCollectionProxy.getTableName(customerSpace, systemStore, active);
            }
            hasSystemStore = StringUtils.isNotBlank(tableName);
        }
        if (hasSystemStore) {
            log.info("Found System batch store for entity " + mainEntity());
        } else {
            log.info("No System batch store for entity " + mainEntity());
        }
    }

    private void checkHasReplace(AbstractStep<? extends BaseStepConfiguration> step) {
        if (step.getConfiguration() instanceof BaseProcessEntityStepConfiguration) {
            replace = ((BaseProcessEntityStepConfiguration)step.getConfiguration()).getNeedReplace();
        }
    }

    void checkManyUpdate(AbstractStep<? extends BaseStepConfiguration> step) {
        Long existingCount = null;
        Long updateCount = null;
        Long newCount = null;
        Map<BusinessEntity, Long> existingValueMap = step.getMapObjectFromContext(BaseWorkflowStep.EXISTING_RECORDS,
                BusinessEntity.class, Long.class);
        if (existingValueMap != null) {
            existingCount = existingValueMap.get(mainEntity());
        }
        Map<BusinessEntity, Long> newValueMap = step.getMapObjectFromContext(BaseWorkflowStep.NEW_RECORDS,
                BusinessEntity.class, Long.class);
        if (newValueMap != null) {
            newCount = newValueMap.get(mainEntity());
        }
        Map<BusinessEntity, Long> updateValueMap = step.getMapObjectFromContext(BaseWorkflowStep.UPDATED_RECORDS,
                BusinessEntity.class, Long.class);
        if (updateValueMap != null) {
            updateCount = updateValueMap.get(mainEntity());
        }

        diffCount = (newCount == null ? 0L : newCount) + (updateCount == null ? 0L : updateCount);
        if (existingCount != null && existingCount != 0L) {
            diffRate = diffCount * 1.0F / existingCount;
            hasManyUpdate = diffRate >= 0.3;
        }
    }

    protected boolean shouldSoftDelete(AbstractStep<? extends BaseStepConfiguration> step) {
        if (!hasSoftDelete || replace) {
            return false;
        }
        ChoreographerContext grapherContext = step.getObjectFromContext(CHOREOGRAPHER_CONTEXT_KEY,
                ChoreographerContext.class);
        boolean hasBatchStore;
        switch (mainEntity()) {
            case Account:
                hasBatchStore = grapherContext.isHasAccountBatchStore();
                break;
            case Contact:
                hasBatchStore = grapherContext.isHasContactBatchStore();
                break;
            case Transaction:
                hasBatchStore = grapherContext.isHasTransactionRawStore();
                break;
            default:
                hasBatchStore = false;
                break;
        }
        return hasBatchStore;
    }

    protected boolean shouldMerge(AbstractStep<? extends BaseStepConfiguration> step) {
        return hasImports || (checkHasEntityMatchRematch(step) && hasBatchStore)
                || checkForceSystemStore(step);
    }

    protected boolean shouldReset(AbstractStep<? extends BaseStepConfiguration> step) {
        if (!hasBatchStore && !hasImports) {
            log.info("No batch store and no imports, going to reset entity.");
            return true;
        }
        return false;
    }

    protected boolean shouldRebuild(AbstractStep<? extends BaseStepConfiguration> step) {
        if (reset) {
            log.info("Going to reset " + mainEntity() + ", skipping rebuild.");
            return false;
        }
        if (replace) {
            log.info("Has replace action in " + mainEntity() + ", going to rebuild");
            return true;
        }
        if (enforceRebuild) {
            log.info("Enforced to rebuild " + mainEntity());
            return true;
        } else if (hasSchemaChange) {
            log.info("Detected schema change in " + mainEntity() + ", going to rebuild");
            return true;
        } else if (hasImports && !hasActiveServingStore) {
            log.info("Has imports but no service store, going to rebuild " + mainEntity());
            return true;
        } else if (hasManyUpdate) {
            log.info("Has more than 30% update, going to rebuild " + mainEntity());
            return true;
        } else if (rebuildDueToActions) {
            return true;
        } else if (shouldSoftDelete(step)) {
            log.info("Common check, no other reason to rebuild, but soft delete happens for " + mainEntity());
            return true;
        }
        log.info("Common check: no reason to rebuild " + mainEntity());
        return false;
    }

    protected boolean shouldUpdate(AbstractStep<? extends BaseStepConfiguration> step) {
        if (reset) {
            log.info("Going to reset " + mainEntity() + ", skipping update.");
            return false;
        }
        if (replace) {
            log.info("Going to replace  " + mainEntity() + ", skipping update.");
            return false;
        }
        if (!rebuild && hasImports) {
            log.info("No going to rebuild but has imports, going to update " + mainEntity());
            return true;
        }
        log.info("Common check: no reason to update " + mainEntity());
        return false;
    }

    boolean hasAnyChange() {
        return rebuild || update;
    }

    protected abstract AbstractStep<?> softDeleteStep();

    protected abstract AbstractStep<?> mergeStep();

    protected abstract AbstractStep<?> cloneStep();

    protected abstract AbstractStep<?> resetStep();

    protected abstract AbstractWorkflow<?> updateWorkflow();

    protected abstract AbstractWorkflow<?> rebuildWorkflow();

    protected abstract BusinessEntity mainEntity();

    // used to skip subworkflow in pa
    protected abstract boolean skipsStepInSubWorkflow(AbstractStep<? extends BaseStepConfiguration> step, int seq);

    protected boolean checkHasAccounts(AbstractStep<? extends BaseStepConfiguration> step) {
        DataCollection.Version active = step.getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        String customerSpace = step.getObjectFromContext(CUSTOMER_SPACE, String.class);
        String rawTableName = dataCollectionProxy.getTableName(customerSpace, //
                TableRoleInCollection.ConsolidatedAccount, active.complement());
        if (StringUtils.isBlank(rawTableName)) {
            rawTableName = dataCollectionProxy.getTableName(customerSpace, //
                    TableRoleInCollection.ConsolidatedAccount, active);
        }
        boolean hasAccounts = StringUtils.isNotBlank(rawTableName);
        if (hasAccounts) {
            log.info("Found account batch store.");
        } else {
            log.info("No account batch store.");
        }
        return hasAccounts;
    }

    protected boolean checkHasEntityMatchRematch(AbstractStep<? extends BaseStepConfiguration> step) {
        return step.getObjectFromContext(FULL_REMATCH_PA, Boolean.class) && step.getObjectFromContext(ENTITY_MATCH_ENABLED, Boolean.class);
    }

    protected boolean checkForceSystemStore(AbstractStep<? extends BaseStepConfiguration> step) {
        return mainEntity().getSystemBatchStore() != null && !hasSystemStore && hasBatchStore
                && step.getObjectFromContext(ENTITY_MATCH_ENABLED, Boolean.class);
    }

    /**
     * Currently only Account entity could have embedded account from
     * contact/transaction import when entity match is turned on
     *
     * @param step
     * @return
     */
    protected boolean hasEmbeddedEntity(AbstractStep<? extends BaseStepConfiguration> step) {
        return false;
    }
}
