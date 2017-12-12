package com.latticeengines.cdl.workflow.choreographers;

import static com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep.CDL_ACTIVE_VERSION;
import static com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep.CONSOLIDATE_INPUT_IMPORTS;
import static com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep.CUSTOMER_SPACE;
import static com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep.ENTITIES_WITH_SCHEMA_CHANGE;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.BaseProcessEntityStepConfiguration;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.workflow.exposed.build.AbstractStep;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.BaseChoreographer;

public abstract class AbstractProcessEntityChoreographer extends BaseChoreographer {

    private static final Logger log = LoggerFactory.getLogger(AbstractProcessEntityChoreographer.class);

    private boolean initialized = false;

    private boolean enforceRebuild = false;
    private boolean hasImports = false;
    private boolean hasSchemaChange = false;
    protected boolean hasActiveServingStore = false;

    private boolean rebuild = false;
    private boolean update = false;

    @Inject
    protected DataCollectionProxy dataCollectionProxy;

    /**
     * Steps that can be skipped based on common entity processing pattern
     */
    protected boolean isCommonSkip(AbstractStep<? extends BaseStepConfiguration> step, int seq) {
        String msg = String.format("Skip step [%d] %s", seq, step.name());

        if (isMergeStep(step)) {
            initialize(step);
            return false;
        }

        if (isCloneStep(step)) {
            checkSchemaChange(step);
            rebuild = shouldRebuild();
            update = shouldUpdate();
            log.info("rebuild=" + rebuild + ", update=" + update + ", entity=" + mainEntity());
            if (rebuild && update) {
                throw new IllegalStateException("Rebuild and update cannot be both true");
            }
            return !shouldClone();
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

        return false;
    }

    private boolean isMergeStep(AbstractStep<? extends BaseStepConfiguration> step) {
        return step.name().endsWith(mergeStep().name());
    }

    private boolean isCloneStep(AbstractStep<? extends BaseStepConfiguration> step) {
        return step.name().endsWith(cloneStep().name());
    }

    private boolean belongsToUpdate(int seq) {
        String namespace = getStepNamespace(seq);
        return updateWorkflow() != null && namespace.contains(updateWorkflow().name());
    }

    private boolean belongsToRebuild(int seq) {
        String namespace = getStepNamespace(seq);
        return rebuildWorkflow() != null && namespace.contains(rebuildWorkflow().name());
    }

    private void initialize(AbstractStep<? extends BaseStepConfiguration> step) {
        if (!initialized) {
            checkEnforcedRebuild(step);
            checkImports(step);
            checkActiveServingStore(step);
            initialized = true;
        }
    }

    private void checkEnforcedRebuild(AbstractStep<? extends BaseStepConfiguration> step) {
        BaseProcessEntityStepConfiguration configuration = (BaseProcessEntityStepConfiguration) step.getConfiguration();
        enforceRebuild = Boolean.TRUE.equals(configuration.getRebuild());
    }

    private void checkImports(AbstractStep<? extends BaseStepConfiguration> step) {
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

    private boolean shouldClone() {
        if (rebuild) {
            log.info("Skip cloning " + mainEntity() + " because going to rebuild and merge is enabled.");
            return false;
        } else {
            return true;
        }
    }

    private boolean shouldRebuild() {
        if (enforceRebuild) {
            log.info("Enforced to rebuild " + mainEntity());
            return true;
        } else if (hasSchemaChange) {
            log.info("Detected schema change in " + mainEntity() + ", going to rebuild");
            return true;
        } else if (hasImports && !hasActiveServingStore) {
            log.info("Has imports but no service store, going to rebuild " + mainEntity());
            return true;
        }
        log.info("No reason to rebuild " + mainEntity());
        return false;
    }

    private boolean shouldUpdate() {
        if (!rebuild && hasImports) {
            log.info("No going to rebuild but has imports, going to update " + mainEntity());
            return true;
        }
        log.info("No reason to update " + mainEntity());
        return false;
    }

    protected abstract AbstractStep mergeStep();

    protected abstract AbstractStep cloneStep();

    protected abstract AbstractWorkflow updateWorkflow();

    protected abstract AbstractWorkflow rebuildWorkflow();

    protected abstract BusinessEntity mainEntity();

}
