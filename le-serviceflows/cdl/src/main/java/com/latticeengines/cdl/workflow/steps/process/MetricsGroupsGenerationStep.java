package com.latticeengines.cdl.workflow.steps.process;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroup;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ActivityStreamSparkStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.ActivityStoreSparkIOMetadata;
import com.latticeengines.domain.exposed.spark.cdl.ActivityStoreSparkIOMetadata.Details;
import com.latticeengines.domain.exposed.spark.cdl.DeriveActivityMetricGroupJobConfig;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.job.cdl.MetricsGroupGenerator;

@Component("metricsGroupsGenerationStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Lazy
public class MetricsGroupsGenerationStep extends RunSparkJob<ActivityStreamSparkStepConfiguration, DeriveActivityMetricGroupJobConfig> {

    private static final Logger log = LoggerFactory.getLogger(MetricsGroupsGenerationStep.class);

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    private DataCollection.Version inactive;

    @Override
    protected Class<? extends AbstractSparkJob<DeriveActivityMetricGroupJobConfig>> getJobClz() {
        return MetricsGroupGenerator.class;
    }

    @Override
    protected DeriveActivityMetricGroupJobConfig configureJob(ActivityStreamSparkStepConfiguration stepConfiguration) {
        List<AtlasStream> streams = new ArrayList<>(stepConfiguration.getActivityStreamMap().values());
        List<ActivityMetricsGroup> groups = new ArrayList<>(stepConfiguration.getActivityMetricsGroupMap().values());
        for (ActivityMetricsGroup group : groups) {
            log.info("Retrieved group {}", group.getGroupId());
        }
        if (CollectionUtils.isEmpty(streams) || CollectionUtils.isEmpty(groups)) {
            log.info("No groups to generate for tenant {}. Skip generating metrics groups", customerSpace);
            return null;
        }
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);

        ActivityStoreSparkIOMetadata inputMetadata = new ActivityStoreSparkIOMetadata();
        Map<String, Details> detailsMap = new HashMap<>();
        List<String> periodStoreTableCtxNames = new ArrayList<>();
        int idx = 0;
        for (AtlasStream stream : streams) {
            String streamId = stream.getStreamId();
            List<String> periods = stream.getPeriods();

            Details details = new Details();
            details.setStartIdx(idx);
            details.setLabels(periods);
            detailsMap.put(streamId, details);

            periodStoreTableCtxNames.addAll(periods.stream().map(periodName -> String.format(PERIOD_STORE_TABLE_FORMAT, streamId, periodName)).collect(Collectors.toList()));
            idx += periods.size();
        }
        inputMetadata.setMetadata(detailsMap);

        List<String> tablenames = periodStoreTableCtxNames.stream().map(this::getStringValueFromContext).collect(Collectors.toList());
        log.info("Fetching periodStore tables with names {}", tablenames);
        List<DataUnit> inputs = getTableSummariesFromCtxKeys(customerSpace.toString(), periodStoreTableCtxNames).stream().map(table ->
                table.partitionedToHdfsDataUnit(table.getName(), Collections.singletonList(InterfaceName.PeriodId.name()))
        ).collect(Collectors.toList());
        validateInputTableCountMatch(groups, inputs, stepConfiguration);

        DeriveActivityMetricGroupJobConfig config = new DeriveActivityMetricGroupJobConfig();
        config.inputMetadata = inputMetadata;
        config.activityMetricsGroups = groups;
        config.evaluationDate = getStringValueFromContext(CDL_EVALUATION_DATE);
        config.setInput(inputs);
        return config;
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        String outputMetadataStr = result.getOutput();
        log.info("Generated output metadata: {}", outputMetadataStr);
        log.info("Generated {} output metrics tables", result.getTargets().size());
        Map<String, Details> outputMetadata = JsonUtils.deserialize(outputMetadataStr, ActivityStoreSparkIOMetadata.class).getMetadata();
        Map<String, String> signatureTableNames = new HashMap<>();
        outputMetadata.forEach((groupId, details) -> {
            HdfsDataUnit metricsGroupDU = result.getTargets().get(details.getStartIdx());
            String name = String.format(METRICS_GROUP_TABLE_FORMAT, groupId);
            Table metricsGroupTable = toTable(name, metricsGroupDU);
            metadataProxy.createTable(customerSpace.toString(), name, metricsGroupTable);
            signatureTableNames.put(groupId, name); // use groupId as signature
            putStringValueInContext(name, metricsGroupTable.getName());
        });
        dataCollectionProxy.upsertTablesWithSignatures(customerSpace.toString(), signatureTableNames, TableRoleInCollection.MetricsGroup, inactive);
    }

    private void validateInputTableCountMatch(List<ActivityMetricsGroup> groups, List<DataUnit> inputs, ActivityStreamSparkStepConfiguration stepConfiguration) {
        Set<String> streamIds = groups.stream().map(group -> group.getStream().getStreamId()).collect(Collectors.toSet());
        List<AtlasStream> streams = streamIds.stream().map(streamId -> stepConfiguration.getActivityStreamMap().get(streamId)).collect(Collectors.toList());
        int expectedPeriodStoresCount = streams.stream().mapToInt(stream -> stream.getPeriods().size()).sum();
        int actualPeriodStoresCount = inputs.size();
        log.info("Required total of {} period tables.", expectedPeriodStoresCount);
        log.info("Found total of {} period tables.", actualPeriodStoresCount);
        if (expectedPeriodStoresCount != actualPeriodStoresCount) {
            throw new IllegalStateException(String.format("Actual number of period tables not match. tenant=%s", customerSpace));
        }
    }
}
