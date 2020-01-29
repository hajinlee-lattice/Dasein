package com.latticeengines.cdl.workflow.steps.maintenance;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MERGE_IMPORTS;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_SOFT_DELETE_TXFMR;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

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

@Component(SoftDeleteActivityStream.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class SoftDeleteActivityStream extends BaseActivityStreamStep<ProcessActivityStreamStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(SoftDeleteActivityStream.class);

    static final String BEAN_NAME = "softDeleteActivityStream";

    private static final String RAWSTREAM_TABLE_PREFIX_FORMAT = "SD_RawStream_%s";
    private static final List<String> RAWSTREAM_PARTITION_KEYS = ImmutableList.of(InterfaceName.__StreamDateId.name());

    @Inject
    protected DataCollectionProxy dataCollectionProxy;

    protected DataCollection.Version active;
    protected DataCollection.Version inactive;
    protected BusinessEntity entity;
    protected TableRoleInCollection batchStore;

    List<Action> softDeleteActions;


    @Override
    protected TransformationWorkflowConfiguration executePreTransformation() {
        initializeConfiguration();
        return generateWorkflowConf();
    }

    @Override
    protected void onPostTransformationCompleted() {
        if (configuration.isRematchMode()) {
            return;
        }
        Map<String, String> rawStreamTables = buildRawStreamBatchStore();
        exportToS3AndAddToContext(rawStreamTables, RAW_ACTIVITY_STREAM_TABLE_NAME);
        updateEntityValueMapInContext(PERFORM_SOFT_DELETE, Boolean.TRUE, Boolean.class);
        if (MapUtils.isNotEmpty(rawStreamTables)) {
            putObjectInContext(RAW_STREAM_TABLE_AFTER_DELETE, rawStreamTables);
        }
    }

    @Override
    protected void initializeConfiguration() {
        super.initializeConfiguration();
        customerSpace = configuration.getCustomerSpace();
        entity = configuration.getMainEntity();
        batchStore = entity.getBatchStore();
        active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
        softDeleteActions = getListObjectFromContext(SOFT_DEELETE_ACTIONS, Action.class);
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
        if (configuration.isRematchMode()) {
            log.info("Activity Stream use replace mode. Skip soft delete!");
            return null;
        }
        if (CollectionUtils.isEmpty(softDeleteActions)) {
            log.info("No soft delete action detected, Skip soft delete!");
            return null;
        }

        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName(BEAN_NAME);

        List<TransformationStepConfig> steps = new ArrayList<>();

        int softDeleteMergeStep = 0;
        TransformationStepConfig mergeSoftDelete = mergeSoftDelete(softDeleteActions);
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


    TransformationStepConfig mergeSoftDelete(List<Action> softDeleteActions) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(TRANSFORMER_MERGE_IMPORTS);
        softDeleteActions.forEach(action -> {
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

    TransformationStepConfig softDelete(int mergeSoftDeleteStep, String batchTableName, String targetPrefix) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(TRANSFORMER_SOFT_DELETE_TXFMR);
        step.setInputSteps(Collections.singletonList(mergeSoftDeleteStep));
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

    protected <V> void updateEntityValueMapInContext(String key, V value, Class<V> clz) {
        updateEntityValueMapInContext(entity, key, value, clz);
    }

    protected <V> void updateEntityValueMapInContext(BusinessEntity entity, String key, V value, Class<V> clz) {
        Map<BusinessEntity, V> entityValueMap = getMapObjectFromContext(key, BusinessEntity.class, clz);
        if (entityValueMap == null) {
            entityValueMap = new HashMap<>();
        }
        entityValueMap.put(entity, value);
        putObjectInContext(key, entityValueMap);
    }
}
