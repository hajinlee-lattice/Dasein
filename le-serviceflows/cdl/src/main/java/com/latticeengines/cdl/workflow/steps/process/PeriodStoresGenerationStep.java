package com.latticeengines.cdl.workflow.steps.process;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HashUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ActivityStreamSparkStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.ActivityStoreSparkIOMetadata;
import com.latticeengines.domain.exposed.spark.cdl.ActivityStoreSparkIOMetadata.Details;
import com.latticeengines.domain.exposed.spark.cdl.DailyStoreToPeriodStoresJobConfig;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.PeriodProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.job.cdl.PeriodStoresGenerator;

@Component("periodStoresGenerationStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Lazy
public class PeriodStoresGenerationStep extends RunSparkJob<ActivityStreamSparkStepConfiguration, DailyStoreToPeriodStoresJobConfig> {

    private static final Logger log = LoggerFactory.getLogger(PeriodStoresGenerationStep.class);

    private static final String INPUT_TABLE_PREFIX = "DAILYSTORE_%s_"; // streamId

    @Inject
    private PeriodProxy periodProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    private DataCollection.Version inactive;


    @Override
    protected Class<? extends AbstractSparkJob<DailyStoreToPeriodStoresJobConfig>> getJobClz() {
        return PeriodStoresGenerator.class;
    }

    @Override
    protected DailyStoreToPeriodStoresJobConfig configureJob(ActivityStreamSparkStepConfiguration stepConfiguration) {
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
        Set<String> skippedStreamIds = getSkippedStreamIds();
        DailyStoreToPeriodStoresJobConfig config = new DailyStoreToPeriodStoresJobConfig();
        config.streams = stepConfiguration.getActivityStreamMap().values()
                .stream().filter(stream -> !skippedStreamIds.contains(stream.getStreamId())).collect(Collectors.toList());
        config.evaluationDate = periodProxy.getEvaluationDate(customerSpace.toString());
        config.businessCalendar = periodProxy.getBusinessCalendar(customerSpace.toString());
        Map<String, Table> dailyDeltaTables = getTablesFromMapCtxKey(customerSpace.toString(), DAILY_ACTIVITY_STREAM_DELTA_TABLE_NAME);
        config.incrementalStreams = config.streams.stream()
                .filter(stream -> shouldIncrUpdate() && dailyDeltaTables.get(stream.getStreamId()) != null)
                .map(AtlasStream::getStreamId).collect(Collectors.toSet());

        log.info("Generating period stores. tenant: {}; evaluation date: {}", customerSpace, config.evaluationDate);

        List<DataUnit> inputs = new ArrayList<>();

        // streamId -> dailyStore table
        Map<String, Table> dailyStoreTables = getTablesFromMapCtxKey(customerSpace.toString(), AGG_DAILY_ACTIVITY_STREAM_TABLE_NAME);
        // PERIODSTORE_<streamId>_<period> -> table name
        Map<String, String> periodStoreBatchTableNames = getPeriodStoreBatchTableNames(config.incrementalStreams);
        if (MapUtils.isEmpty(dailyStoreTables)) {
            log.info("No daily stores found for tenant {}. Skip generating period stores", customerSpace);
            return null;
        }
        Map<String, String> periodStoreTableNames = getMapObjectFromContext(PERIOD_STORE_TABLE_NAME, String.class, String.class);
        if (periodStoreTableNames != null) {
            if (allTablesExist(periodStoreTableNames)) {
                log.info("[{}] Period stores have been created before retry. Skip generating period stores", customerSpace);
                dataCollectionProxy.upsertTablesWithSignatures(customerSpace.toString(), periodStoreTableNames, TableRoleInCollection.PeriodStores, inactive);
                return null;
            }
        }

        ActivityStoreSparkIOMetadata inputMetadata = new ActivityStoreSparkIOMetadata();
        Map<String, Details> metadata = new HashMap<>();
        config.streams.forEach(stream -> {
            String streamId = stream.getStreamId();
            Details details = new Details();
            details.setStartIdx(inputs.size());
            if (config.incrementalStreams.contains(streamId)) {
                List<String> labels = new ArrayList<>();
                configuration.getActivityStreamMap().get(streamId).getPeriods().forEach(period -> {
                    String key = String.format(PERIOD_STORE_TABLE_FORMAT, streamId, period);
                    String batchTableName = periodStoreBatchTableNames.get(key);
                    if (StringUtils.isNotBlank(batchTableName)) {
                        Table batchStoreTable = metadataProxy.getTable(customerSpace.toString(), batchTableName);
                        inputs.add(batchStoreTable.partitionedToHdfsDataUnit(null, Collections.singletonList(InterfaceName.PeriodId.name())));
                        labels.add(period);
                    } else {
                        log.info("Stream {} is set to incremental update but no period batch table found. Delta table will be taken as new batch store.", streamId);
                        config.streamsWithNoBatch.add(streamId);
                    }
                    Table dailyStoreDeltaTable = dailyDeltaTables.get(streamId);
                    if (dailyStoreDeltaTable == null) {
                        throw new IllegalStateException(String.format("Stream %s is set to incremental update but no import delta table found", streamId));
                    }
                    inputs.add(dailyStoreDeltaTable.partitionedToHdfsDataUnit(null, Collections.singletonList(InterfaceName.PeriodId.name())));
                    log.info(String.format("Added delta table %s to stream %s for incremental update.", dailyStoreDeltaTable.getName(), streamId));
                });
                details.setLabels(labels);
                metadata.put(streamId, details);
            } else {
                Table dailyStoreTable = dailyStoreTables.get(streamId);
                if (dailyStoreTable == null) {
                    throw new IllegalStateException(String.format("Cannot find the daily store table for stream %s", streamId));
                }
                DataUnit tableDU = dailyStoreTable.partitionedToHdfsDataUnit(String.format(INPUT_TABLE_PREFIX, streamId) + dailyStoreTable.getName(), Collections.singletonList(InterfaceName.__StreamDateId.name()));
                inputs.add(tableDU);
                metadata.put(streamId, details);
            }
        });
        config.setInput(inputs);
        inputMetadata.setMetadata(metadata);
        config.inputMetadata = inputMetadata;
        return config;
    }

    private Map<String, String> getPeriodStoreBatchTableNames(Set<String> incrementalStreams) {
        if (CollectionUtils.isEmpty(incrementalStreams)) {
            return Collections.emptyMap();
        }
        Map<String, AtlasStream> streams = configuration.getActivityStreamMap();
        List<String> signatures = new ArrayList<>();
        incrementalStreams.forEach(streamId -> {
            streams.get(streamId).getPeriods().forEach(period -> {
                signatures.add(String.format(PERIOD_STORE_TABLE_FORMAT, streamId, period));
            });
        });
        return dataCollectionProxy.getTableNamesWithSignatures(customerSpace.toString(), TableRoleInCollection.PeriodStores, inactive.complement(), signatures);
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        String outputMetadataStr = result.getOutput();
        log.info("Generated output metadata: {}", outputMetadataStr);
        log.info("Generated {} output metrics tables", result.getTargets().size());
        Map<String, Details> metadata = JsonUtils.deserialize(outputMetadataStr, ActivityStoreSparkIOMetadata.class).getMetadata();
        Map<String, Table> signatureTables = new HashMap<>();
        metadata.forEach((streamId, details) -> {
            for (int offset = 0; offset < details.getLabels().size(); offset++) {
                String period = details.getLabels().get(offset);
                String ctxKey = String.format(PERIOD_STORE_TABLE_FORMAT, streamId, period);
                String tableName = TableUtils.getFullTableName(ctxKey, HashUtils.getCleanedString(UuidUtils.shortenUuid(UUID.randomUUID())));
                Table periodStoreTable = dirToTable(tableName, result.getTargets().get(details.getStartIdx() + offset));
                metadataProxy.createTable(customerSpace.toString(), tableName, periodStoreTable);
                signatureTables.put(ctxKey, periodStoreTable); // use ctxKey name as signature
            }
        });
        Map<String, String> signatureTableNames = exportToS3AndAddToContext(signatureTables, PERIOD_STORE_TABLE_NAME);
        dataCollectionProxy.upsertTablesWithSignatures(customerSpace.toString(), signatureTableNames, TableRoleInCollection.PeriodStores, inactive);
    }

    private Set<String> getSkippedStreamIds() {
        if (!hasKeyInContext(ACTIVITY_STREAMS_SKIP_AGG)) {
            return Collections.emptySet();
        }

        Set<String> skippedStreamIds = getSetObjectFromContext(ACTIVITY_STREAMS_SKIP_AGG, String.class);
        log.info("Stream IDs skipped for period stores generation = {}", skippedStreamIds);
        return skippedStreamIds;
    }

    private boolean shouldIncrUpdate() {
        return !configuration.isShouldRebuild();
    }
}
