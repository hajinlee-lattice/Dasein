package com.latticeengines.cdl.workflow.steps.maintenance;

import static com.latticeengines.domain.exposed.admin.LatticeModule.TalkingPoint;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;

@Lazy
@Component(SoftDeleteAccount.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class SoftDeleteAccount extends BaseSingleEntitySoftDelete<ProcessAccountStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(SoftDeleteAccount.class);

    static final String BEAN_NAME = "softDeleteAccount";

    @Inject
    private BatonService batonService;

    @Value("${cdl.processAnalyze.skip.dynamo.publication}")
    private boolean skipPublishDynamo;

    @Override
    protected PipelineTransformationRequest getConsolidateRequest() {

        List<Action> actions = filterSoftDeleteActions(softDeleteActions, BusinessEntity.Account);
        if (CollectionUtils.isEmpty(actions)) {
            log.info("No suitable delete actions for Account. Skip this step.");
            return null;
        } else {
            log.info("Filtered actions: " + JsonUtils.serialize(actions));
            PipelineTransformationRequest request = new PipelineTransformationRequest();
            request.setName("SoftDeleteAccount");

            List<TransformationStepConfig> steps = new ArrayList<>();
            TransformationStepConfig mergeSoftDelete = mergeSoftDelete(actions, InterfaceName.AccountId.name());
            steps.add(mergeSoftDelete);
            int softDeleteMergeStep = steps.size() - 1;
            if (hasSystemStore()) {
                TransformationStepConfig softDeleteSystemBatchStore = softDeleteSystemBatchStore(softDeleteMergeStep);
                steps.add(softDeleteSystemBatchStore);
            }
            TransformationStepConfig softDelete = softDelete(softDeleteMergeStep);
            steps.add(softDelete);

            setOutputTablePrefix(steps);

            request.setSteps(steps);

            return request;
        }
    }

    @Override
    protected boolean skipRegisterBatchStore() {
        return false;
    }

    @Override
    protected boolean processSystemBatchStore() {
        return true;
    }

    protected boolean shouldPublishDynamo() {
        boolean enableTp = batonService.hasModule(customerSpace, TalkingPoint);
        return !skipPublishDynamo || enableTp;
    }

}
