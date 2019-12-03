package com.latticeengines.cdl.workflow.steps.process;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.cdl.workflow.steps.rebuild.ProfileStepBase;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
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

        Set<String> servingEntities = getObjectFromContext(ACTIVITY_MERGED_METRICS_SERVING_ENTITIES, Set.class);
        log.info("Found metrics serving entities from context: {}", servingEntities);
        if (CollectionUtils.isEmpty(servingEntities)) {
            return null;
        }
        // for every tables run Profile->Bucket->CalcStats
        int profileStep = 0;
        int bucketStep = 1;
        List<TransformationStepConfig> steps = new ArrayList<>();
        for (String servingEntity : servingEntities) {
            String tableCtxName = String.format(MERGED_METRICS_GROUP_TABLE_FORMAT, String.format("%s_%s", getEntityLevel().name(), servingEntity));
            String tableName = getStringValueFromContext(tableCtxName);
            if (StringUtils.isBlank(tableName)) {
                log.info("No need to profile {} for {}", servingEntity, getEntityLevel());
                continue;
            }
            profiledTableNames.put(servingEntity, tableName);
            steps.add(profile(tableName));
            steps.add(bucket(profileStep, tableName, getBucketTablePrefix(servingEntity)));
            steps.add(calcStats(profileStep, bucketStep, getStatsTablePrefix(servingEntity), null));
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
                exportToDynamo(tableName, servingStore.getPrimaryKey().name(), null);
                String statsTableName = constructStatsTableName(getStatsTablePrefix(servingEntity.name()));
                log.info("Adding stats table to context: {}", statsTableName);
                updateEntityValueMapInContext(servingEntity, STATS_TABLE_NAMES, statsTableName, String.class);
            }
        }
        // TODO - enrich table attrs
    }

    private String getStatsTablePrefix(String servingEntity) {
        return String.format("%s%s", servingEntity, "Stats");
    }

    private String getBucketTablePrefix(String servingEntity) {
        return String.format("%s%s", servingEntity, "Buckets");
    }
}
