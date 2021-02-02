package com.latticeengines.cdl.workflow.steps.maintenance;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MERGE_TS_DELETE_TXFMR;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_SOFT_DELETE_TXFMR;
import static com.latticeengines.domain.exposed.query.EntityType.MarketingActivity;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.DeleteActionConfiguration;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessActivityStreamStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.spark.cdl.MergeTimeSeriesDeleteDataConfig;
import com.latticeengines.domain.exposed.spark.cdl.SoftDeleteConfig;
import com.latticeengines.domain.exposed.util.TableUtils;

@Component(SoftDeleteActivityStream.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class SoftDeleteActivityStream extends BaseDeleteActivityStream<ProcessActivityStreamStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(SoftDeleteActivityStream.class);

    static final String BEAN_NAME = "softDeleteActivityStream";

    private List<Action> softDeleteActivityActions;

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
        Map<String, String> rawStreamTables = updateRawStreamTableEntries();
        updateEntityValueMapInContext(PERFORM_SOFT_DELETE, Boolean.TRUE, Boolean.class);
        if (MapUtils.isNotEmpty(rawStreamTables)) {
            exportToS3AndAddToContext(rawStreamsAfterDelete, RAW_STREAM_TABLE_AFTER_DELETE);
            log.info("Put updated raw stream table in context: {}", rawStreamsAfterDelete);
        }
    }

    private Map<String, String> updateRawStreamTableEntries() {
        Map<String, String> updated = new HashMap<>(getActiveRawStreamTables());
        log.info("current pipeline version: {}", pipelineVersion);
        log.info("original raw stream tables: {}", updated);
        rawStreamsAfterDelete = rawStreamsAfterDelete.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> TableUtils.getFullTableName(entry.getValue(), pipelineVersion)));
        log.info("raw stream tables after delete: {}", rawStreamsAfterDelete);
        updated.putAll(rawStreamsAfterDelete);
        log.info("updated raw stream tables entries: {}", updated);
        return updated;
    }

    @Override
    protected List<Action> deleteActions() {
        return softDeleteActivityActions;
    }

    @Override
    protected String getBeanName() {
        return BEAN_NAME;
    }

    @Override
    protected Boolean needPartition() {
        return Boolean.TRUE;
    }

    @Override
    protected void initializeConfiguration() {
        super.initializeConfiguration();
        List<Action> deleteActions = getListObjectFromContext(SOFT_DELETE_ACTIONS, Action.class);
        softDeleteActivityActions = CollectionUtils.isEmpty(deleteActions) ? Collections.emptyList() : deleteActions.stream().filter(deleteAction -> {
            DeleteActionConfiguration config = (DeleteActionConfiguration) deleteAction.getActionConfiguration();
            return config.hasEntity(BusinessEntity.ActivityStream);
        }).collect(Collectors.toList());
    }

    @Override
    protected PipelineTransformationRequest getConsolidateRequest() {
        if (configuration.isRematchMode()) {
            log.info("Activity Stream use replace mode. Skip soft delete!");
            return null;
        }
        if (CollectionUtils.isEmpty(softDeleteActivityActions)) {
            log.info("No delete action detected, Skip soft delete!");
            return null;
        }
        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName(getBeanName());

        List<TransformationStepConfig> steps = new ArrayList<>();

        getActiveRawStreamTables().forEach((streamId, tableName) -> {
            List<DeleteActionConfiguration> deleteConfigsForStream = softDeleteActivityActions.stream().filter(action -> {
                DeleteActionConfiguration config = (DeleteActionConfiguration) action.getActionConfiguration();
                return config.hasStream(streamId);
            }).map(action -> (DeleteActionConfiguration) action.getActionConfiguration()).collect(Collectors.toList());
            String idColumn = configuration.getActivityStreamMap().get(streamId).getAggrEntities()
                    .contains(BusinessEntity.Contact.name()) ? InterfaceName.ContactId.name() : InterfaceName.AccountId.name();

            TransformationStepConfig mergeTimeSeriesDeleteStep = getMergeTimeSeriesDeleteStep(idColumn, deleteConfigsForStream);
            if (mergeTimeSeriesDeleteStep != null) {
                List<ColumnMetadata> cms = metadataProxy.getTableColumns(customerSpace.toString(), tableName);
                boolean hasId = cms.stream().anyMatch(cm -> idColumn.equals(cm.getAttrName()));
                if (!hasId) {
                    // requested to delete stream by ID, but stream doesn't have the ID, skip deleting step
                    log.warn("Stream {} does not have the deletion id {}", streamId, idColumn);
                    return;
                }
                steps.add(mergeTimeSeriesDeleteStep);
            }
            appendSoftDeleteStreamStep(steps, mergeTimeSeriesDeleteStep != null, deleteConfigsForStream, idColumn, streamId, tableName);
        });

        if (steps.isEmpty()) {
            log.info("No suitable delete actions for any stream.");
            return null;
        } else {
            request.setSteps(steps);
            return request;
        }
    }

    private Map<String, String> getActiveRawStreamTables() {
        if (Boolean.TRUE.equals(getObjectFromContext(ACTIVITY_PARTITION_MIGRATION_PERFORMED, Boolean.class))) {
            return getMapObjectFromContext(ACTIVITY_MIGRATED_RAW_STREAM, String.class, String.class);
        }
        return configuration.getActiveRawStreamTables();
    }

    private void appendSoftDeleteStreamStep(List<TransformationStepConfig> steps, boolean hasDeleteImport, List<DeleteActionConfiguration> deleteConfigsForStream, String idColumn, String streamId, String rawStreamTable) {
        SoftDeleteConfig softDeleteConfig = new SoftDeleteConfig();
        softDeleteConfig.setIdColumn(idColumn);
        softDeleteConfig.setPartitionKeys(RAWSTREAM_PARTITION_KEYS);
        softDeleteConfig.setNeedPartitionOutput(needPartition());
        softDeleteConfig.setEventTimeColumn(InterfaceName.StreamDateId.name());
        softDeleteConfig.setTimeRangesColumn(InterfaceName.TimeRanges.name());
        softDeleteConfig.setTimeRangesToDelete(extractGlobalTimeRanges(deleteConfigsForStream));

        TransformationStepConfig step = new TransformationStepConfig();
        String targetTablePrefix = String.format(RAWSTREAM_TABLE_PREFIX_FORMAT, streamId);
        step.setTransformer(TRANSFORMER_SOFT_DELETE_TXFMR);
        step.setTargetPartitionKeys(RAWSTREAM_PARTITION_KEYS);
        setTargetTable(step, targetTablePrefix);
        rawStreamsAfterDelete.put(streamId, targetTablePrefix);

        addBaseTables(step, ImmutableList.of(RAWSTREAM_PARTITION_KEYS), rawStreamTable);
        if (hasDeleteImport) {
            log.info("Add batch store table {} and delete import table for soft delete.", rawStreamTable);
            step.setInputSteps(Collections.singletonList(steps.size() - 1));
            softDeleteConfig.setDeleteSourceIdx(0);
        } else {
            log.info("Add batch store table {} for soft delete by only time ranges.", rawStreamTable);
        }
        step.setConfiguration(appendEngineConf(softDeleteConfig, lightEngineConfig()));
        steps.add(step);
    }

    private Set<List<Long>> extractGlobalTimeRanges(List<DeleteActionConfiguration> deleteConfigsForStream) {
        return deleteConfigsForStream.stream().filter(config -> StringUtils.isBlank(config.getDeleteDataTable())).map(config -> {
            Preconditions.checkArgument(StringUtils.isNotBlank(config.getFromDate()),
                    "Delete action need to have either delete data or time range. Missing FromDate in time range");
            Preconditions.checkArgument(StringUtils.isNotBlank(config.getToDate()),
                    "Delete action need to have either delete data or time range. Missing ToDate in time range");
            return DateTimeUtils.parseTimeRange(config.getFromDate(), config.getToDate());
        }).collect(Collectors.toSet());
    }

    private TransformationStepConfig getMergeTimeSeriesDeleteStep(String joinKey, List<DeleteActionConfiguration> deleteConfigsForStream) {
        if (deleteConfigsForStream.stream().noneMatch(deleteConfig -> StringUtils.isNotBlank(deleteConfig.getDeleteDataTable()))) {
            // no delete import, skip merging step
            return null;
        }
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(TRANSFORMER_MERGE_TS_DELETE_TXFMR);
        Map<Integer, List<Long>> timeRanges = new HashMap<>();
        Map<Integer, String> deleteIDs = new HashMap<>();
        int timeRangeIdx = 0;
        for (DeleteActionConfiguration deleteConfig : deleteConfigsForStream) {
            if (StringUtils.isNotBlank(deleteConfig.getDeleteDataTable())) {
                // only merge delete with delete import, skip ones deleting only by time range
                addBaseTables(step, deleteConfig.getDeleteDataTable());
                if (StringUtils.isNotBlank(deleteConfig.getFromDate()) && StringUtils.isNotBlank(deleteConfig.getToDate())) {
                    timeRanges.put(timeRangeIdx, DateTimeUtils.parseTimeRange(deleteConfig.getFromDate(), deleteConfig.getToDate()));
                }
                if (MarketingActivity.equals(deleteConfig.getDeleteEntityType())) {
                    deleteIDs.put(timeRangeIdx, getDeleteId(deleteConfig.getIdEntity(), joinKey));
                }
                timeRangeIdx++;
            }
        }
        MergeTimeSeriesDeleteDataConfig config = new MergeTimeSeriesDeleteDataConfig();
        config.joinKey = joinKey;
        config.timeRanges.putAll(timeRanges);
        //add join table if joinkey is not the same with delete entity id for marketing
        if (deleteIDs.values().stream().anyMatch(deleteId -> !deleteId.equals(joinKey))) {
            config.deleteIDs.putAll(deleteIDs);
            config.joinTableIdx = timeRangeIdx;
            addBaseTables(step, consolidatedContactTable);
        }
        step.setConfiguration((appendEngineConf(config, lightEngineConfig())));
        return step;
    }

    private String getDeleteId(BusinessEntity entity, String joinKey) {
        switch (entity) {
            case Account:
                return InterfaceName.AccountId.name();
            case Contact:
                return InterfaceName.ContactId.name();
            default:
                return joinKey;
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
