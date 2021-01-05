package com.latticeengines.cdl.workflow.steps.merge;

import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.AggregatedActivityStream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
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
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.metadata.retention.RetentionPolicy;
import com.latticeengines.domain.exposed.metadata.retention.RetentionPolicyTimeUnit;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ActivityStreamSparkStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.MigrateActivityPartitionKeyJobConfig;
import com.latticeengines.domain.exposed.spark.cdl.SparkIOMetadataWrapper;
import com.latticeengines.domain.exposed.util.RetentionPolicyUtil;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.job.cdl.MigrateActivityPartitionKeyJob;


@Component(MigrateActivityPartitionKey.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Lazy
public class MigrateActivityPartitionKey extends RunSparkJob<ActivityStreamSparkStepConfiguration, MigrateActivityPartitionKeyJobConfig> {

    private static final Logger log = LoggerFactory.getLogger(MigrateActivityPartitionKey.class);

    public static final String BEAN_NAME = "migrateActivityPartitionKey";

    private static final String legacyStreamDateIdPartitionKey = InterfaceName.__StreamDateId.name();
    private static final String MIGRATED_TABLE_FORMAT = "MIGRATED_%s"; // + original table name

    Map<String, String> activeDailyStores;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Override
    protected Class<? extends AbstractSparkJob<MigrateActivityPartitionKeyJobConfig>> getJobClz() {
        return MigrateActivityPartitionKeyJob.class;
    }

    @Override
    protected MigrateActivityPartitionKeyJobConfig configureJob(ActivityStreamSparkStepConfiguration stepConfiguration) {
        if (alreadyMigrated()) {
            log.info("Tenant already migrated to use new partition key. Skip");
            return null;
        }
        DataCollection.Version active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        // for each stream, find rawStream and dailyStore
        List<AtlasStream> streamsWithActiveTable = stepConfiguration.getActivityStreamMap().values().stream()
                .filter(stream -> stepConfiguration.getActiveRawStreamTables().containsKey(stream.getStreamId())).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(streamsWithActiveTable)) {
            log.info("No active stores. Skip migration step.");
            updateMigrationStatus();
            return null;
        }
        activeDailyStores = dataCollectionProxy
                .getTableNamesWithSignatures(customerSpace.toString(), AggregatedActivityStream, active,
                        streamsWithActiveTable.stream().map(AtlasStream::getStreamId).collect(Collectors.toList()));
        SparkIOMetadataWrapper inputMetadataWrapper = new SparkIOMetadataWrapper();
        Map<String, SparkIOMetadataWrapper.Partition> inputMetadata = new HashMap<>();
        List<DataUnit> inputs = new ArrayList<>();
        streamsWithActiveTable.forEach(stream -> {
            SparkIOMetadataWrapper.Partition details = new SparkIOMetadataWrapper.Partition();
            details.setStartIdx(inputs.size());
            details.setLabels(Arrays.asList(legacyStreamDateIdPartitionKey, legacyStreamDateIdPartitionKey)); // both tables use the same partition key
            inputMetadata.put(stream.getStreamId(), details);
            String activeRawStreamTableName = stepConfiguration.getActiveRawStreamTables().get(stream.getStreamId());
            Table activeRawStreamTable = metadataProxy.getTable(customerSpace.toString(), activeRawStreamTableName);
            if (activeRawStreamTable == null) {
                throw new IllegalStateException(String.format("Failed to get active raw stream table with name: %s", activeRawStreamTableName));
            }
            String activeDailyStoreName = activeDailyStores.get(stream.getStreamId());
            Table activeDailyStore = metadataProxy.getTable(customerSpace.toString(), activeDailyStoreName);
            if (activeDailyStore == null) {
                throw new IllegalStateException(String.format("Failed to get active daily store with name: %s", activeDailyStoreName));
            }
            inputs.add(activeRawStreamTable.partitionedToHdfsDataUnit(null, Collections.singletonList(legacyStreamDateIdPartitionKey)));
            inputs.add(activeDailyStore.partitionedToHdfsDataUnit(null, Collections.singletonList(legacyStreamDateIdPartitionKey)));
        });
        inputMetadataWrapper.setMetadata(inputMetadata);
        MigrateActivityPartitionKeyJobConfig config = new MigrateActivityPartitionKeyJobConfig();
        config.setInput(inputs);
        config.inputMetadata = inputMetadataWrapper;
        return config;
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        Map<String, SparkIOMetadataWrapper.Partition> outputMetadata = JsonUtils.deserialize(result.getOutput(), SparkIOMetadataWrapper.class).getMetadata();
        Map<String, String> migratedRawStreamTables = new HashMap<>();
        Map<String, String> migratedDailyStoreTables = new HashMap<>();
        String version = HashUtils.getCleanedString(UuidUtils.shortenUuid(UUID.randomUUID()));
        RetentionPolicy retentionPolicy = createRetentionPolicy();
        outputMetadata.forEach((streamId, details) -> {
            HdfsDataUnit migratedRawStreamDU = result.getTargets().get(details.getStartIdx());
            String migratedRawStreamName = TableUtils.getFullTableName(String.format(MIGRATED_TABLE_FORMAT, configuration.getActiveRawStreamTables().get(streamId)), version);
            migratedRawStreamTables.put(streamId, migratedRawStreamName);

            HdfsDataUnit migratedDailyStoreDU = result.getTargets().get(details.getStartIdx() + 1);
            String migratedDailyStoreName = TableUtils.getFullTableName(String.format(MIGRATED_TABLE_FORMAT, activeDailyStores.get(streamId)), version);
            migratedDailyStoreTables.put(streamId, migratedDailyStoreName);

            metadataProxy.createTempTable(customerSpace.toString(), migratedRawStreamName, dirToTable(migratedRawStreamName, migratedRawStreamDU), retentionPolicy);
            metadataProxy.createTempTable(customerSpace.toString(), migratedDailyStoreName, dirToTable(migratedDailyStoreName, migratedDailyStoreDU), retentionPolicy);
        });
        updateMigrationStatus();
        putObjectInContext(ACTIVITY_PARTITION_MIGRATION_PERFORMED, Boolean.TRUE);
        putObjectInContext(ACTIVITY_MIGRATED_RAW_STREAM, migratedRawStreamTables);
        putObjectInContext(ACTIVITY_MIGRATED_DAILY_STREAM, migratedDailyStoreTables);
    }

    private RetentionPolicy createRetentionPolicy() {
        return RetentionPolicyUtil.toRetentionPolicy(2, RetentionPolicyTimeUnit.DAY);
    }

    private boolean alreadyMigrated() {
        DataCollectionStatus status = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        return status.isActivityPartitionKeyMigrated();
    }

    private void updateMigrationStatus() {
        DataCollectionStatus status = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        status.setActivityPartitionKeyMigrated(true);
        putObjectInContext(CDL_COLLECTION_STATUS, status);
    }
}
