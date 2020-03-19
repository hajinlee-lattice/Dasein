package com.latticeengines.cdl.workflow.steps.process;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.cdl.workflow.steps.rebuild.ProfileStepBase;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.domain.exposed.workflow.BaseWrapperStepConfiguration;

abstract class ProfileActivityMetricsStepBase<T extends BaseWrapperStepConfiguration> extends ProfileStepBase<T> {

    private static final Logger log = LoggerFactory.getLogger(ProfileActivityMetricsStepBase.class);

    private Map<String, String> profiledTableNames = new HashMap<>();

    protected abstract BusinessEntity getEntityLevel(); // Account/Contact. For constructing ActivityMetrics table name only

    @Override
    protected BusinessEntity getEntity() { // serving entity. need to be configured before use
        return null;
    } // serving multiple entities

    protected abstract String getRequestName();

    @Override
    protected TransformationWorkflowConfiguration executePreTransformation() {
        initializeConfiguration();
        PipelineTransformationRequest request = getTransformRequest();
        return request != null ? transformationProxy.getWorkflowConf(customerSpace.toString(), request, getConfiguration().getPodId()) : null;
    }

    private void initializeConfiguration() {
        configuration = getConfiguration();
        customerSpace = configuration.getCustomerSpace();
    }

    @SuppressWarnings("unchecked")
    private PipelineTransformationRequest getTransformRequest() {
        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName(getRequestName());
        request.setSubmitter(customerSpace.getTenantId());
        request.setKeepTemp(false);
        request.setEnableSlack(false);

        Set<String> servingEntityNames = getObjectFromContext(ACTIVITY_MERGED_METRICS_SERVING_ENTITIES, Set.class);
        if (CollectionUtils.isEmpty(servingEntityNames)) {
            log.info("No activity serving entity found. Skip profiling activity metrics.");
            return null;
        }
        Set<BusinessEntity> servingEntities = servingEntityNames.stream().map(BusinessEntity::getByName).collect(Collectors.toSet());
        log.info("Found metrics serving entities from context: {}", servingEntities);
        if (CollectionUtils.isEmpty(servingEntities)) {
            return null;
        }
        // for every tables run Profile->Bucket->CalcStats
        int profileStep = 0;
        int bucketStep = 1;
        List<TransformationStepConfig> steps = new ArrayList<>();
        Map<String, String> mergedMetricsGroupTableNames = getMapObjectFromContext(MERGED_METRICS_GROUP_TABLE_NAME, String.class, String.class);
        for (BusinessEntity servingEntity : servingEntities) {
            TableRoleInCollection servingStore = servingEntity.getServingStore();
            String tableCtxName = String.format("%s_%s", getEntityLevel().name(), servingStore.name());
            String tableName = mergedMetricsGroupTableNames.get(tableCtxName);
            if (noNeedToProfile(tableCtxName, tableName)) {
                log.info("No need to profile serving entity: {}, serving store: {}", servingEntity, servingStore);
                continue;
            }
            profiledTableNames.put(servingEntity.name(), tableName);
            steps.add(profile(tableName));
            steps.add(bucket(profileStep, tableName, getBucketTablePrefix(servingEntity.name())));
            steps.add(calcStats(profileStep, bucketStep, getStatsTablePrefix(servingEntity.name()), null));
            profileStep += 3;
            bucketStep += 3;
        }
        log.info("{} activity metrics need to be profiled for {}: {}", profiledTableNames.size(), getEntityLevel(), profiledTableNames);

        if (CollectionUtils.isEmpty(steps)) {
            return null;
        }
        request.setSteps(steps);
        return request;
    }

    private boolean noNeedToProfile(String tableCtxName, String tableName) {
        if (StringUtils.isNotBlank(tableName)) {
            Long numRecord = getMergedCount(tableName);
            if (numRecord <= 0) { // pbc does not support profiling empty tables
                log.warn("{} rows found in table {}. Skip profiling", numRecord, tableName);
                return true;
            } else {
                log.info("{} rows found in table {}", numRecord, tableName);
                return false;
            }
        }
        log.warn("No table name found in context {}. Skip profiling", tableCtxName);
        return true;
    }

    private Long getMergedCount(String tableName) {
        Table table = metadataProxy.getTableSummary(customerSpace.toString(), tableName);
        return table.getExtracts().get(0).getProcessedRecords();
    }

    private String constructStatsTableName(String statsNamePrefix) {
        return TableUtils.getFullTableName(statsNamePrefix, getStringValueFromContext(TRANSFORM_PIPELINE_VERSION));
    }

    @Override
    protected void onPostTransformationCompleted() {
        for (Map.Entry<String, String> entry : profiledTableNames.entrySet()) {
            BusinessEntity servingEntity = BusinessEntity.getByName(entry.getKey());
            TableRoleInCollection servingStore = servingEntity.getServingStore();
            if (servingStore != null) {
                String tableName = entry.getValue();
                exportTableRoleToRedshift(tableName, servingStore);
                exportToDynamo(tableName, servingStore.getPartitionKey(), servingStore.getRangeKey());
                String statsTableName = constructStatsTableName(getStatsTablePrefix(servingEntity.name()));
                log.info("Adding stats table to context: {}", statsTableName);
                updateEntityValueMapInContext(servingEntity, STATS_TABLE_NAMES, statsTableName, String.class);
            }
        }
    }

    private String getStatsTablePrefix(String servingEntity) {
        return String.format("%s%s", servingEntity, "Stats");
    }

    private String getBucketTablePrefix(String servingStore) {
        return String.format("%s%s", servingStore, "Buckets");
    }
}
