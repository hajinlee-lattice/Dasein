package com.latticeengines.cdl.workflow.steps.maintenance;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_FILTER_BY_JOIN_TXFMR;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MERGE_IMPORTS;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_SOFT_DELETE_TXFMR;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.latticeengines.cdl.workflow.steps.merge.BaseActivityStreamStep;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
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
import com.latticeengines.domain.exposed.spark.common.FilterByJoinConfig;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

public abstract class BaseDeleteActivityStream<T extends ProcessActivityStreamStepConfiguration>
        extends BaseActivityStreamStep<T> {

    private static final Logger log = LoggerFactory.getLogger(BaseDeleteActivityStream.class);

    protected static final String RAWSTREAM_TABLE_PREFIX_FORMAT = "SD_RawStream_%s";
    protected static final List<String> RAWSTREAM_PARTITION_KEYS = ImmutableList.of(InterfaceName.StreamDateId.name());

    @Inject
    protected DataCollectionProxy dataCollectionProxy;

    @Inject
    protected MetadataProxy metadataProxy;

    protected DataCollection.Version active;
    protected DataCollection.Version inactive;
    protected BusinessEntity entity;
    protected TableRoleInCollection batchStore;
    protected String consolidatedContactTable;

    protected abstract List<Action> deleteActions();

    protected abstract String getBeanName();

    protected abstract Boolean needPartition();

    @Override
    protected void initializeConfiguration() {
        super.initializeConfiguration();
        customerSpace = configuration.getCustomerSpace();
        entity = configuration.getMainEntity();
        batchStore = entity.getBatchStore();
        active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);

        consolidatedContactTable = dataCollectionProxy.getTableName(customerSpace.getTenantId(),
                TableRoleInCollection.ConsolidatedContact, inactive);
        if (StringUtils.isBlank(consolidatedContactTable)) {
            consolidatedContactTable = dataCollectionProxy.getTableName(customerSpace.getTenantId(),
                    TableRoleInCollection.ConsolidatedContact, active);
        }
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

        configuration.getActiveRawStreamTables().forEach((streamId, tableName) -> {
            int initialSteps = steps.size();
            List<TransformationStepConfig> accountSteps = softDeleteSteps(streamId, tableName, BusinessEntity.Account);
            steps.addAll(accountSteps);
            List<TransformationStepConfig> contactSteps = softDeleteSteps(streamId, tableName, BusinessEntity.Contact);
            steps.addAll(contactSteps);
            if (steps.size() > initialSteps) {
                TransformationStepConfig lastStep = steps.get(steps.size() - 1);
                setTargetTable(lastStep, String.format(RAWSTREAM_TABLE_PREFIX_FORMAT, streamId));
            }
        });

        if (steps.isEmpty()) {
            log.info("No suitable delete actions for any stream.");
            return null;
        } else {
            request.setSteps(steps);
            return request;
        }
    }

    private List<TransformationStepConfig> softDeleteSteps(String streamId, String tableName,
            BusinessEntity idEntity) {
        List<TransformationStepConfig> steps = new ArrayList<>();

        String idColumn = BusinessEntity.Account.equals(idEntity) ? InterfaceName.AccountId.name()
                : InterfaceName.ContactId.name();
        List<Action> validActions = deleteActions().stream().filter(action -> {
            DeleteActionConfiguration configuration = (DeleteActionConfiguration) action.getActionConfiguration();
            return configuration.hasStream(streamId) && idEntity.equals(configuration.getIdEntity());
        }).collect(Collectors.toList());

        if (CollectionUtils.isNotEmpty(validActions)) {
            List<ColumnMetadata> cms = metadataProxy.getTableColumns(customerSpace.toString(), tableName);
            boolean hasId = cms.stream().anyMatch(cm -> idColumn.equals(cm.getAttrName()));
            if (hasId) {
                TransformationStepConfig mergeSoftDelete = mergeDeleteActions(validActions, idColumn);
                steps.add(mergeSoftDelete);
                int mergeDeleteStep = steps.size() - 1;
                String rawStreamTablePrefix = String.format(RAWSTREAM_TABLE_PREFIX_FORMAT, streamId);
                if (BusinessEntity.Account.equals(idEntity) && AtlasStream.StreamType.MarketingActivity
                        .equals(configuration.getActivityStreamMap().get(streamId).getStreamType())) {
                    List<String> selectColumns = Arrays.asList(InterfaceName.AccountId.name(),
                            InterfaceName.ContactId.name());
                    TransformationStepConfig joinStep = joinTable(mergeDeleteStep, consolidatedContactTable, idColumn,
                            selectColumns);
                    steps.add(joinStep);
                    mergeDeleteStep = steps.size() - 1;
                }
                TransformationStepConfig softDelete = softDelete(mergeDeleteStep, tableName, idColumn, rawStreamTablePrefix);
                steps.add(softDelete);
                log.info("Add steps to delete from stream {} via {}", streamId, idColumn);
                rawStreamTablePrefixes.put(streamId, rawStreamTablePrefix);
                log.info("Add table prefix {} for stream {}", rawStreamTablePrefix, streamId);
            } else {
                log.warn("Stream {} does not have the deletion id {}", streamId, idColumn);
            }
        } else {
            log.info("No suitable delete actions for stream {}", streamId);
        }

        return steps;
    }

    TransformationStepConfig mergeDeleteActions(List<Action> deleteActions, String joinKey) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(TRANSFORMER_MERGE_IMPORTS);
        deleteActions.forEach(action -> {
            DeleteActionConfiguration configuration = (DeleteActionConfiguration) action.getActionConfiguration();
            addBaseTables(step, configuration.getDeleteDataTable());
        });
        MergeImportsConfig config = new MergeImportsConfig();
        config.setDedupSrc(true);
        config.setJoinKey(joinKey);
        config.setAddTimestamps(false);
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        return step;
    }

    TransformationStepConfig joinTable(int preStep, String joinTableName, String key, List<String> selectColumns) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(TRANSFORMER_FILTER_BY_JOIN_TXFMR);
        step.setInputSteps(Collections.singletonList(preStep));
        if (StringUtils.isNotEmpty(joinTableName)) {
            log.info("Add join Table=" + joinTableName);
            addBaseTables(step, joinTableName);
        } else {
            throw new IllegalArgumentException("The join table is empty for delete!");
        }
        FilterByJoinConfig FilterByJoinConfig = new FilterByJoinConfig();
        FilterByJoinConfig.setKey(key);
        FilterByJoinConfig.setJoinType("inner");
        FilterByJoinConfig.setSelectColumns(selectColumns);
        FilterByJoinConfig.setSwitchSide(true);
        step.setConfiguration(appendEngineConf(FilterByJoinConfig, lightEngineConfig()));
        return step;
    }

    TransformationStepConfig softDelete(int mergeDeleteStep, String batchTableName, String idColumn,
                                        String targetPrefix) {
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
        softDeleteConfig.setIdColumn(idColumn); // id column in delete file and stream file
        softDeleteConfig.setPartitionKeys(RAWSTREAM_PARTITION_KEYS);
        softDeleteConfig.setNeedPartitionOutput(needPartition());
        step.setTargetPartitionKeys(RAWSTREAM_PARTITION_KEYS);
        step.setConfiguration(appendEngineConf(softDeleteConfig, lightEngineConfig()));
        return step;
    }

}
