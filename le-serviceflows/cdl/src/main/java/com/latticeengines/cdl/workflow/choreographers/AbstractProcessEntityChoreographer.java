package com.latticeengines.cdl.workflow.choreographers;

import static com.latticeengines.workflow.exposed.build.BaseWorkflowStep.CDL_ACTIVE_VERSION;
import static com.latticeengines.workflow.exposed.build.BaseWorkflowStep.CHOREOGRAPHER_CONTEXT_KEY;
import static com.latticeengines.workflow.exposed.build.BaseWorkflowStep.CONSOLIDATE_INPUT_IMPORTS;
import static com.latticeengines.workflow.exposed.build.BaseWorkflowStep.CUSTOMER_SPACE;
import static com.latticeengines.workflow.exposed.build.BaseWorkflowStep.ENTITIES_WITH_SCHEMA_CHANGE;
import static com.latticeengines.workflow.exposed.build.BaseWorkflowStep.PROCESS_ANALYTICS_DECISIONS_KEY;

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
    boolean jobImpacted = false;
    private boolean initialized = false;
    private boolean hasBatchStore = false;
    private float diffRate = 0;

    boolean rebuild = false;
    boolean update = false;
    boolean reset = false;

    @Inject
    protected DataCollectionProxy dataCollectionProxy;

    /**
     * Steps that can be skipped based on common entity processing pattern
     */
    boolean isCommonSkip(AbstractStep<? extends BaseStepConfiguration> step, int seq) {
        String msg = String.format("Skip step [%d] %s", seq, step.name());

        if (isMergeStep(step)) {
            initialize(step);
            return !shouldMerge(step);
        }

        if (isCloneStep(step)) {
            checkSchemaChange(step);
            checkManyUpdate(step);
            reset = shouldReset(step);
            rebuild = shouldRebuild(step);
            update = shouldUpdate();
            log.info("reset=" + reset + ", rebuild=" + rebuild + ", update=" + update + ", entity=" + mainEntity());
            saveDecisions(step);
            if (reset && (rebuild || update)) {
                throw new IllegalStateException("When reset, neither rebuild nor update can be true.");
            }
            if (rebuild && update) {
                throw new IllegalStateException("Rebuild and update cannot be both true");
            }
        }

        if (belongsToUpdate(seq)) {
            update = shouldUpdate();
            if (!update) {
                log.info(msg + ", because not in update mode.");
                return true;
            }
        }

        if (belongsToRebuild(seq)) {
            rebuild = shouldRebuild(step);
            if (!rebuild) {
                log.info(msg + ", because not in rebuild mode.");
                return true;
            }
        }

        if (isResetStep(step)) {
            reset = shouldReset(step);
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

    private void saveDecisions(AbstractStep<? extends BaseStepConfiguration> step) {
        TreeSet<String> decisions = new TreeSet<>();
        decisions.add(reset ? "reset=true" : (update ? "update=true" : "rebuild=true"));
        decisions.add(enforceRebuild ? "enforceRebuild=true" : "");
        decisions.add(hasSchemaChange ? "hasSchemaChange=true" : "");
        decisions.add(hasImports ? "hasImports=true" : "");
        decisions.add(hasManyUpdate ? "hasManyUpdate=true" : "");
        decisions.add(hasManyUpdate ? String.format("diffRate=%f", diffRate) : "");
        decisions.add(jobImpacted ? "jobImpacted=true" : "");
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
        checkActiveServingStore(step);
        checkHasBatchStore(step);
        checkJobImpactedEntity(step);
    }

    void checkJobImpactedEntity(AbstractStep<? extends BaseStepConfiguration> step) {
        ChoreographerContext grapherContext = step.getObjectFromContext(CHOREOGRAPHER_CONTEXT_KEY,
                ChoreographerContext.class);
        if (CollectionUtils.isNotEmpty(grapherContext.getJobImpactedEntities())
                && grapherContext.getJobImpactedEntities().contains(mainEntity())) {
            jobImpacted = true;
        }
        log.info("Job impacted=" + jobImpacted + " for " + mainEntity());
    }

    private void checkEnforcedRebuild(AbstractStep<? extends BaseStepConfiguration> step) {
        BaseProcessEntityStepConfiguration configuration = (BaseProcessEntityStepConfiguration) step.getConfiguration();
        enforceRebuild = Boolean.TRUE.equals(configuration.getRebuild());
    }

    private void checkImports(AbstractStep<? extends BaseStepConfiguration> step) {
        @SuppressWarnings("rawtypes")
        Map<BusinessEntity, List> entityImportsMap = step.getMapObjectFromContext(CONSOLIDATE_INPUT_IMPORTS,
                BusinessEntity.class, List.class);
        hasImports = MapUtils.isNotEmpty(entityImportsMap) && entityImportsMap.containsKey(mainEntity());
        if (hasImports) {
            log.info("Found imports for " + mainEntity().name());
        } else {
            log.info("Found no imports for " + mainEntity().name());
        }
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

        long diffCount = (newCount == null ? 0L : newCount) + (updateCount == null ? 0L : updateCount);
        if (existingCount != null && existingCount != 0L) {
            diffRate = diffCount * 1.0F / existingCount;
            hasManyUpdate = diffRate >= 0.3;
        }
    }

    protected boolean shouldMerge(AbstractStep<? extends BaseStepConfiguration> step) {
        return hasImports;
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
        } else if (jobImpacted) {
            return true;
        }
        log.info("Common check: no reason to rebuild " + mainEntity());
        return false;
    }

    protected boolean shouldUpdate() {
        if (reset) {
            log.info("Going to reset " + mainEntity() + ", skipping update.");
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
}
