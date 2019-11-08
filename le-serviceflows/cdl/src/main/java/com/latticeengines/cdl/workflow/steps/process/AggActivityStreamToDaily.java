package com.latticeengines.cdl.workflow.steps.process;

import static com.latticeengines.domain.exposed.cdl.activity.DimensionGenerator.DimensionGeneratorOption.HASH;
import static com.latticeengines.domain.exposed.cdl.activity.StreamDimension.Usage.Dedup;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.__StreamDateId;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.AggregatedActivityStream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.DimensionCalculator;
import com.latticeengines.domain.exposed.cdl.activity.DimensionMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ActivityStreamSparkStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.AggDailyActivityConfig;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.job.cdl.AggDailyActivityJob;

@Component(AggActivityStreamToDaily.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Lazy
public class AggActivityStreamToDaily
        extends RunSparkJob<ActivityStreamSparkStepConfiguration, AggDailyActivityConfig> {

    private static final Logger log = LoggerFactory.getLogger(AggActivityStreamToDaily.class);

    static final String BEAN_NAME = "aggActivityStreamToDaily";

    private static final String DAILY_STORE_TABLE_FORMAT = "DailyStream_%s_%s";
    private static final TypeReference<Map<String, Map<String, DimensionMetadata>>> METADATA_MAP_TYPE = new TypeReference<Map<String, Map<String, DimensionMetadata>>>() {
    };

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Override
    protected AggDailyActivityConfig configureJob(ActivityStreamSparkStepConfiguration stepConfiguration) {
        if (MapUtils.isEmpty(stepConfiguration.getActivityStreamMap())) {
            return null;
        }

        Map<String, AtlasStream> streams = stepConfiguration.getActivityStreamMap();
        Map<String, Table> rawStreamTableNames = getTablesFromMapCtxKey(customerSpace.toString(),
                RAW_ACTIVITY_STREAM_TABLE_NAME);

        AggDailyActivityConfig config = new AggDailyActivityConfig();
        // set dimensions
        config.dimensionMetadataMap = getTypedObjectFromContext(STREAM_DIMENSION_METADATA_MAP, METADATA_MAP_TYPE);
        streams.values().forEach(stream -> {
            String streamId = stream.getStreamId();
            Map<String, DimensionCalculator> calculatorMap = new HashMap<>();
            Set<String> hashDimensions = new HashSet<>();
            List<String> additionalDimAttrs = new ArrayList<>(getEntityIds(stream));
            stream.getDimensions().forEach(dimension -> {
                calculatorMap.put(dimension.getName(), dimension.getCalculator());
                if (dimension.getGenerator().getOption() == HASH) {
                    hashDimensions.add(dimension.getName());
                } else if (dimension.getUsages() != null && dimension.getUsages().contains(Dedup)) {
                    additionalDimAttrs.add(dimension.getName());
                }
            });

            config.attrDeriverMap.put(streamId,
                    stream.getAttributeDerivers() == null ? Collections.emptyList() : stream.getAttributeDerivers());
            config.dimensionCalculatorMap.put(streamId, calculatorMap);
            config.hashDimensionMap.put(streamId, hashDimensions);
            config.additionalDimAttrMap.put(streamId, additionalDimAttrs);
        });
        // set input
        List<DataUnit> units = new ArrayList<>();
        rawStreamTableNames.forEach((streamId, table) -> {
            Preconditions.checkArgument(CollectionUtils.size(table.getExtracts()) == 1,
                    String.format("Table %s should only have one extract, got %d", table.getName(),
                            CollectionUtils.size(table.getExtracts())));
            Extract extract = table.getExtracts().get(0);
            config.rawStreamInputIdx.put(streamId, units.size());
            HdfsDataUnit unit = new HdfsDataUnit();
            unit.setName(streamId);
            unit.setPath(extract.getPath());
            unit.setCount(extract.getProcessedRecords());
            // TODO maybe centralize partition key somewhere
            unit.setPartitionKeys(Collections.singletonList(__StreamDateId.name()));
            units.add(unit);
        });
        config.setInput(units);
        log.info("Agg daily activity stream config = {}", JsonUtils.serialize(config));
        return config;
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        List<?> rawList = JsonUtils.deserialize(result.getOutput(), List.class);
        List<String> streamIdsInOutput = JsonUtils.convertList(rawList, String.class);
        Preconditions.checkNotNull(streamIdsInOutput);
        Preconditions.checkArgument(streamIdsInOutput.size() == result.getTargets().size(),
                "Result target list size should be the same as streamId list size");

        Map<String, HdfsDataUnit> dailyAggUnits = IntStream.range(0, result.getTargets().size()) //
                .mapToObj(idx -> Pair.of(streamIdsInOutput.get(idx), result.getTargets().get(idx))) //
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        log.info("Daily aggregate data units = {}", JsonUtils.serialize(dailyAggUnits));

        // create daily tables
        Map<String, Table> dailyAggTables = dailyAggUnits.entrySet().stream().map(entry -> {
            String streamId = entry.getKey();
            String tableName = String.format(DAILY_STORE_TABLE_FORMAT, streamId,
                    UuidUtils.shortenUuid(UUID.randomUUID()));
            Table dailyAggTable = dirToTable(tableName, entry.getValue());
            metadataProxy.createTable(configuration.getCustomer(), tableName, dailyAggTable);
            return Pair.of(streamId, dailyAggTable);
        }).collect(Collectors.toMap(Pair::getKey, Pair::getValue));

        // link table to role in collection
        DataCollection.Version inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
        Map<String, String> dailyTableNames = exportToS3AndAddToContext(dailyAggTables,
                AGG_DAILY_ACTIVITY_STREAM_TABLE_NAME);
        dataCollectionProxy.upsertTablesWithSignatures(configuration.getCustomer(), dailyTableNames,
                AggregatedActivityStream, inactive);
        log.info("Daily aggregate activity stream tables = {}, version = {}", dailyTableNames, inactive);
    }

    private List<String> getEntityIds(@NotNull AtlasStream stream) {
        if (CollectionUtils.isEmpty(stream.getAggrEntities())) {
            return Collections.emptyList();
        }

        List<String> aggrEntities = stream.getAggrEntities();
        if (aggrEntities.contains(BusinessEntity.Contact.name())) {
            return Arrays.asList(InterfaceName.ContactId.name(), InterfaceName.AccountId.name());
        } else if (aggrEntities.contains(BusinessEntity.Account.name())) {
            return Collections.singletonList(InterfaceName.AccountId.name());
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    protected Class<? extends AbstractSparkJob<AggDailyActivityConfig>> getJobClz() {
        return AggDailyActivityJob.class;
    }
}
