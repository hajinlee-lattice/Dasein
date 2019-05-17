package com.latticeengines.cdl.workflow.choreographers;

import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.RebuildProductWorkflow;
import com.latticeengines.cdl.workflow.UpdateProductWorkflow;
import com.latticeengines.cdl.workflow.steps.merge.MergeProduct;
import com.latticeengines.cdl.workflow.steps.rebuild.ProfileProduct;
import com.latticeengines.cdl.workflow.steps.rebuild.ProfileProductHierarchy;
import com.latticeengines.cdl.workflow.steps.reset.ResetProduct;
import com.latticeengines.cdl.workflow.steps.update.CloneProduct;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractStep;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;
import com.latticeengines.workflow.exposed.build.Choreographer;

@Component
public class ProcessProductChoreographer extends AbstractProcessEntityChoreographer implements Choreographer {
    private static final Logger log = LoggerFactory.getLogger(ProcessProductChoreographer.class);

    @Inject
    private MergeProduct mergeProduct;

    @Inject
    private CloneProduct cloneProduct;

    @Inject
    private ResetProduct resetProduct;

    @Inject
    private UpdateProductWorkflow updateProductWorkflow;

    @Inject
    private RebuildProductWorkflow rebuildProductWorkflow;

    boolean hasChange = false;

    @Override
    protected AbstractStep<?> mergeStep() {
        return mergeProduct;
    }

    @Override
    protected AbstractStep<?> cloneStep() {
        return cloneProduct;
    }

    @Override
    protected AbstractStep<?> resetStep() {
        return resetProduct;
    }

    @Override
    protected AbstractWorkflow<?> updateWorkflow() {
        return updateProductWorkflow;
    }

    @Override
    protected AbstractWorkflow<?> rebuildWorkflow() {
        return rebuildProductWorkflow;
    }

    @Override
    protected BusinessEntity mainEntity() {
        return BusinessEntity.Product;
    }

    @Override
    protected boolean shouldUpdate() {
        // product should never enter update mode
        return false;
    }

    @Override
    protected boolean shouldRebuild(AbstractStep<? extends BaseStepConfiguration> step) {
        if (reset) {
            log.info("Going to reset " + mainEntity() + ", skipping rebuild.");
            return false;
        }
        if (enforceRebuild) {
            log.info("Enforced to rebuild " + mainEntity());
            return true;
        } else if (hasSchemaChange) {
            log.info("Detected schema change in " + mainEntity() + ", going to rebuild.");
            return true;
        } else if (hasImports) {
            log.info("Has product imports always rebuild " + mainEntity());
            return true;
        }
        log.info("No reason to rebuild " + mainEntity());
        return false;
    }

    @Override
    public boolean skipStep(AbstractStep<? extends BaseStepConfiguration> step, int seq) {

        boolean skip;
        skip = isCommonSkip(step, seq);
        if (!skip) {
            BusinessEntity entity = null;
            if (isProfileProduct(step)) {
                entity = BusinessEntity.Product;
            } else if (isProfileProductHierarchy(step)) {
                entity = BusinessEntity.ProductHierarchy;
            }

            if (entity != null) {
                Map<BusinessEntity, Integer> entityValueMap;
                try {
                    entityValueMap = step.getMapObjectFromContext(BaseWorkflowStep.FINAL_RECORDS, BusinessEntity.class,
                            Integer.class);
                } catch (Exception e) {
                    entityValueMap = null;
                }
                if (entityValueMap == null) {
                    skip = true;
                    log.info("Change to skip because entityValueMap is null.");
                } else {
                    Integer finalRecords = entityValueMap.getOrDefault(entity, 0);
                    if (finalRecords == 0) {
                        skip = true;
                        log.info("Change to skip because finalRecords is " + String.valueOf(finalRecords));
                    }
                }
                if (skip) {
                    updateFlags(seq, entity);
                } else {
                    hasChange = true;
                }
            }
        }

        return skip;
    }

    private void updateFlags(int seq, BusinessEntity entity) {
        if (belongsToUpdate(seq)) {
            update = false;
        } else if (belongsToRebuild(seq)) {
            rebuild = false;
        }
        log.info("Skip was changed to true for " + entity);
    }

    private boolean isProfileProduct(AbstractStep<? extends BaseStepConfiguration> step) {
        return step.name().equalsIgnoreCase(ProfileProduct.BEAN_NAME);
    }

    private boolean isProfileProductHierarchy(AbstractStep<? extends BaseStepConfiguration> step) {
        return step.name().equalsIgnoreCase(ProfileProductHierarchy.BEAN_NAME);
    }

    @Override
    protected boolean skipsStepInSubWorkflow(AbstractStep<? extends BaseStepConfiguration> step, int seq) {
        return false;
    }

}
