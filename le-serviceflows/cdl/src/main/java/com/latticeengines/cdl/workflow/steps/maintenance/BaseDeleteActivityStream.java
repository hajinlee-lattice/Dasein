package com.latticeengines.cdl.workflow.steps.maintenance;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MERGE_IMPORTS;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_SOFT_DELETE_TXFMR;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.latticeengines.cdl.workflow.steps.merge.BaseActivityStreamStep;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.DeleteActionConfiguration;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessActivityStreamStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.spark.cdl.MergeImportsConfig;
import com.latticeengines.domain.exposed.spark.cdl.SoftDeleteConfig;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;

public abstract class BaseDeleteActivityStream<T extends ProcessActivityStreamStepConfiguration>
        extends BaseActivityStreamStep<T> {

    private static final Logger log = LoggerFactory.getLogger(BaseDeleteActivityStream.class);

    private static final String RAWSTREAM_TABLE_PREFIX_FORMAT = "SD_RawStream_%s";
    private static final List<String> RAWSTREAM_PARTITION_KEYS = ImmutableList.of(InterfaceName.__StreamDateId.name());

    @Inject
    protected DataCollectionProxy dataCollectionProxy;

    protected DataCollection.Version active;
    protected DataCollection.Version inactive;
    protected BusinessEntity entity;
    protected TableRoleInCollection batchStore;

    protected abstract List<Action> deleteActions();
    protected abstract String getBeanName();


    @Override
    protected void initializeConfiguration() {
        super.initializeConfiguration();
        customerSpace = configuration.getCustomerSpace();
        entity = configuration.getMainEntity();
        batchStore = entity.getBatchStore();
        active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
    }

    protected TransformationWorkflowConfiguration generateWorkflowConf() {
        PipelineTransformationRequest request = getConsolidateRequest();
        if (request == null) {
            return null;
        } else {
            return transformationProxy.getWorkflowConf(customerSpace.toString(), request, configuration.getPodId());
        }
    }

    @Override
    protected PipelineTransformationRequest getConsolidateRequest() {
        if (CollectionUtils.isEmpty(deleteActions())) {
            log.info("No delete action detected, Skip soft delete!");
            return null;
        }

        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName(getBeanName());

        List<TransformationStepConfig> steps = new ArrayList<>();

        int softDeleteMergeStep = 0;
        TransformationStepConfig mergeSoftDelete = mergeDeleteActions(deleteActions());
        steps.add(mergeSoftDelete);
        configuration.getActiveRawStreamTables().forEach((streamId, tableName) -> {
            String rawStreamTablePrefix = String.format(RAWSTREAM_TABLE_PREFIX_FORMAT, streamId);
            TransformationStepConfig softDelete = softDelete(softDeleteMergeStep, tableName, rawStreamTablePrefix);
            steps.add(softDelete);
            rawStreamTablePrefixes.put(streamId, rawStreamTablePrefix);
        });
        request.setSteps(steps);

        return request;
    }

    TransformationStepConfig mergeDeleteActions(List<Action> deleteActions) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(TRANSFORMER_MERGE_IMPORTS);
        deleteActions.forEach(action -> {
            DeleteActionConfiguration configuration = (DeleteActionConfiguration) action.getActionConfiguration();
            addBaseTables(step, configuration.getDeleteDataTable());
        });
        MergeImportsConfig config = new MergeImportsConfig();
        config.setDedupSrc(true);
        config.setJoinKey(InterfaceName.AccountId.name());
        config.setAddTimestamps(false);
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        return step;
    }

    TransformationStepConfig softDelete(int mergeDeleteStep, String batchTableName, String targetPrefix) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(TRANSFORMER_SOFT_DELETE_TXFMR);
        step.setInputSteps(Collections.singletonList(mergeDeleteStep));
        setTargetTable(step, targetPrefix);
        if (StringUtils.isNotEmpty(batchTableName)) {
            log.info("Add masterTable=" + batchTableName);
            addBaseTables(step, ImmutableList.of(RAWSTREAM_PARTITION_KEYS), batchTableName);
        } else {
            throw new IllegalArgumentException("The master table is empty for soft delete!");
        }
        SoftDeleteConfig softDeleteConfig = new SoftDeleteConfig();
        softDeleteConfig.setDeleteSourceIdx(0);
        softDeleteConfig.setIdColumn(InterfaceName.AccountId.name());
        softDeleteConfig.setPartitionKeys(RAWSTREAM_PARTITION_KEYS);
        step.setTargetPartitionKeys(RAWSTREAM_PARTITION_KEYS);
        step.setConfiguration(appendEngineConf(softDeleteConfig, lightEngineConfig()));
        return step;
    }

}
