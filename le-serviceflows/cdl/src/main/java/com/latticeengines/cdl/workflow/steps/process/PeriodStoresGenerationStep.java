package com.latticeengines.cdl.workflow.steps.process;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HashUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
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
        DailyStoreToPeriodStoresJobConfig config = new DailyStoreToPeriodStoresJobConfig();
        config.streams = new ArrayList<>(stepConfiguration.getActivityStreamMap().values());
        config.evaluationDate = periodProxy.getEvaluationDate(customerSpace.toString());
        config.businessCalendar = periodProxy.getBusinessCalendar(customerSpace.toString());

        log.info("Generating period stores. tenant: {}; evaluation date: {}", customerSpace, config.evaluationDate);

        List<DataUnit> inputs = new ArrayList<>();
        int dailyStoreIdx = 0;

        // streamId -> dailyStore table
        Map<String, Table> dailyStoreTables = getTablesFromMapCtxKey(customerSpace.toString(), AGG_DAILY_ACTIVITY_STREAM_TABLE_NAME);
        if (MapUtils.isEmpty(dailyStoreTables)) {
            log.info("No daily stores found for tenant {}. Skip generating period stores", customerSpace);
            return null;
        }
        Map<String, String> periodStoreTableNames = getMapObjectFromContext(PERIOD_STORE_TABLE_NAME, String.class, String.class);
        String customer = customerSpace.toString();
        Map<String, Table> aggDailyStreamTables = periodStoreTableNames.entrySet().stream()
                .map(entry -> Pair.of(entry.getKey(), metadataProxy.getTable(customer, entry.getValue())))
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        boolean shortCutMode = MapUtils.isEmpty(aggDailyStreamTables) ? false : aggDailyStreamTables.values().stream().noneMatch(Objects::isNull);
        if (shortCutMode) {
            dataCollectionProxy.upsertTablesWithSignatures(customerSpace.toString(), periodStoreTableNames, TableRoleInCollection.PeriodStores, inactive);
            return null;
        }
        ActivityStoreSparkIOMetadata inputMetadata = new ActivityStoreSparkIOMetadata();
        Map<String, Details> metadata = new HashMap<>();
        for (Map.Entry<String, Table> entry : dailyStoreTables.entrySet()) {
            String streamId = entry.getKey();
            Table dailyStoreTable = entry.getValue();

            if (dailyStoreTable == null) {
                throw new IllegalStateException(String.format("Cannot find the daily store table for stream %s", streamId));
            }
            DataUnit tableDU = dailyStoreTable.partitionedToHdfsDataUnit(String.format(INPUT_TABLE_PREFIX, streamId) + dailyStoreTable.getName(), Collections.singletonList(InterfaceName.__StreamDateId.name()));
            inputs.add(tableDU);

            Details details = new Details();
            details.setStartIdx(dailyStoreIdx++);
            metadata.put(streamId, details);
        }
        config.setInput(inputs);
        inputMetadata.setMetadata(metadata);
        config.inputMetadata = inputMetadata;
        return config;
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        Map<String, Details> metadata = JsonUtils.deserialize(result.getOutput(), ActivityStoreSparkIOMetadata.class).getMetadata();
        Map<String, String> signatureTableNames = new HashMap<>();
        metadata.forEach((streamId, details) -> {
            for (int offset = 0; offset < details.getLabels().size(); offset++) {
                String period = details.getLabels().get(offset);
                String ctxKey = String.format(PERIOD_STORE_TABLE_FORMAT, streamId, period);
                String tableName = TableUtils.getFullTableName(ctxKey, HashUtils.getCleanedString(UuidUtils.shortenUuid(UUID.randomUUID())));
                Table periodStoreTable = dirToTable(tableName, result.getTargets().get(details.getStartIdx() + offset));
                metadataProxy.createTable(customerSpace.toString(), tableName, periodStoreTable);
                signatureTableNames.put(details.getLabels().get(offset), tableName); // use period name as signature
                putStringValueInContext(ctxKey, periodStoreTable.getName());
            }
        });
        putObjectInContext(PERIOD_STORE_TABLE_NAME, signatureTableNames);
        dataCollectionProxy.upsertTablesWithSignatures(customerSpace.toString(), signatureTableNames, TableRoleInCollection.PeriodStores, inactive);
    }
}
