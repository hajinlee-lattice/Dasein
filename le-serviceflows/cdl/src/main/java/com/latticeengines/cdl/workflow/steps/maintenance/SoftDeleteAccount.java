package com.latticeengines.cdl.workflow.steps.maintenance;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.DynamoExportConfig;

@Lazy
@Component(SoftDeleteAccount.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class SoftDeleteAccount extends BaseSingleEntitySoftDelete<ProcessAccountStepConfiguration> {

    static final String BEAN_NAME = "softDeleteAccount";

    @Override
    protected PipelineTransformationRequest getConsolidateRequest() {

        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName("SoftDeleteAccount");

        List<TransformationStepConfig> steps = new ArrayList<>();

        int softDeleteMergeStep = 0;
        TransformationStepConfig mergeSoftDelete = mergeSoftDelete(softDeleteActions);
        TransformationStepConfig softDeleteSystemBatchStore = softDeleteSystemBatchStore(softDeleteMergeStep);
        TransformationStepConfig softDelete = softDelete(softDeleteMergeStep);
        steps.add(mergeSoftDelete);
        steps.add(softDeleteSystemBatchStore);
        steps.add(softDelete);
        request.setSteps(steps);

        return request;
    }

    @Override
    protected boolean skipRegisterBatchStore() {
        return false;
    }

    @Override
    protected void onPostTransformationCompleted() {
        super.onPostTransformationCompleted();
        String batchStoreTableName = dataCollectionProxy.getTableName(customerSpace.toString(), batchStore, inactive);
        relinkDynamo(batchStoreTableName);
    }

    private void relinkDynamo(String tableName) {
        String inputPath = metadataProxy.getAvroDir(configuration.getCustomerSpace().toString(), tableName);
        DynamoExportConfig config = new DynamoExportConfig();
        config.setTableName(getBatchStoreName());
        config.setLinkTableName(masterTable.getName());
        config.setRelink(Boolean.TRUE);
        config.setPartitionKey(InterfaceName.AccountId.name());
        config.setInputPath(PathUtils.toAvroGlob(inputPath));
        addToListInContext(TABLES_GOING_TO_DYNAMO, config, DynamoExportConfig.class);
    }

    @Override
    protected boolean processSystemBatchStore() {
        return true;
    }
}
