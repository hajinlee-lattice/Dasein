package com.latticeengines.cdl.workflow.steps.process;

import static com.latticeengines.domain.exposed.query.BusinessEntity.Account;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Contact;
import static java.util.Collections.singletonList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.util.HashUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ActivityStreamSparkStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.CalculateLastActivityDateConfig;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.job.cdl.CalculateLastActivityDate;

@Component(GenerateLastActivityDate.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Lazy
public class GenerateLastActivityDate
        extends RunSparkJob<ActivityStreamSparkStepConfiguration, CalculateLastActivityDateConfig> {

    private static final Logger log = LoggerFactory.getLogger(GenerateLastActivityDate.class);

    private static final String LAST_ACTIVITY_TABLE_PREFIX = "LastActivityDate_%s"; // entity

    static final String BEAN_NAME = "generateLastActivityDate";

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    private DataCollection.Version inactive;

    @Override
    protected CalculateLastActivityDateConfig configureJob(ActivityStreamSparkStepConfiguration stepConfiguration) {
        if (isShortCutMode()) {
            log.info("Already computed this step, skip processing (short-cut mode)");
            return null;
        } else if (!shouldExecute()) {
            log.info("Period store is not updated, skip calculating last activity date");
            return null;
        }
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);

        CalculateLastActivityDateConfig config = new CalculateLastActivityDateConfig();
        // entity -> list of table names
        Map<String, List<String>> streamTables = selectPeriodTables();
        Preconditions.checkArgument(MapUtils.isNotEmpty(streamTables),
                "Period store tables for activity store should not be empty here");
        log.info("Selected stream tables for last activity date calculation = {}", streamTables);

        List<DataUnit> inputs = new ArrayList<>();
        List<String> dateAttrs = new ArrayList<>();
        addDataUnits(inputs, streamTables.get(Account.name()), config.accountStreamInputIndices, dateAttrs);
        addDataUnits(inputs, streamTables.get(Contact.name()), config.contactStreamInputIndices, dateAttrs);
        addContactBatchStore(inputs, config, dateAttrs);
        config.setInput(inputs);
        config.dateAttrs = dateAttrs;
        // TODO set purge timestamp
        return config;
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        String outputStr = result.getOutput();
        Map<?, ?> rawMap = JsonUtils.deserialize(outputStr, Map.class);
        Map<String, Integer> entityOutputIdx = JsonUtils.convertMap(rawMap, String.class, Integer.class);
        Preconditions.checkArgument(MapUtils.isNotEmpty(entityOutputIdx),
                "Last activity date output index map should not be empty here");
        // entity -> last activity date table name
        Map<String, String> tableNames = new HashMap<>();
        Map<String, Table> tables = new HashMap<>();
        entityOutputIdx.forEach((entity, outputIdx) -> {
            // create table
            String key = String.format(LAST_ACTIVITY_TABLE_PREFIX, entity);
            String name = TableUtils.getFullTableName(key,
                    HashUtils.getCleanedString(UuidUtils.shortenUuid(UUID.randomUUID())));
            Table table = toTable(name, result.getTargets().get(outputIdx));
            metadataProxy.createTable(configuration.getCustomer(), name, table);
            tableNames.put(entity, name);
            tables.put(entity, table);
        });
        log.info("Last activity date table names = {}", tableNames);
        exportToS3AndAddToContext(tables, LAST_ACTIVITY_DATE_TABLE_NAME);
        tableNames.values().forEach(name -> addToListInContext(TEMPORARY_CDL_TABLES, name, String.class));
    }

    @Override
    protected Class<? extends AbstractSparkJob<CalculateLastActivityDateConfig>> getJobClz() {
        return CalculateLastActivityDate.class;
    }

    /*
     * input helpers for calculation spark job
     */

    private void addContactBatchStore(@NotNull List<DataUnit> inputs, @NotNull CalculateLastActivityDateConfig config,
            @NotNull List<String> dateAttrs) {
        Table contactTable = getContactTable();
        if (contactTable != null) {
            config.contactTableIdx = inputs.size();
            dateAttrs.add(null);
            inputs.add(contactTable.toHdfsDataUnit("Contact"));
        }
    }

    private void addDataUnits(@NotNull List<DataUnit> inputs, @NotNull List<String> tables,
            @NotNull List<Integer> inputIndices, @NotNull List<String> dateAttrs) {
        for (HdfsDataUnit du : toDataUnits(tables)) {
            inputIndices.add(inputs.size());
            // internal date attribute exist for all period stores
            dateAttrs.add(InterfaceName.LastActivityDate.name());
            inputs.add(du);
        }
    }

    private List<HdfsDataUnit> toDataUnits(List<String> tableNames) {
        if (CollectionUtils.isEmpty(tableNames)) {
            return Collections.emptyList();
        }

        return tableNames.stream() //
                .map(name -> metadataProxy.getTable(configuration.getCustomer(), name)) //
                .map(table -> table.partitionedToHdfsDataUnit(null, singletonList(InterfaceName.PeriodId.name()))) //
                .collect(Collectors.toList());
    }

    private Table getContactTable() {
        DataCollection.Version active = inactive.complement();
        Table contactTable = dataCollectionProxy.getTable(customerSpace.toString(), Contact.getBatchStore(), inactive);
        if (contactTable != null) {
            log.info("Using contact batch store {} in inactive version {}", contactTable.getName(), inactive);
            return contactTable;
        }

        contactTable = dataCollectionProxy.getTable(customerSpace.toString(), Contact.getBatchStore(), active);
        if (contactTable != null) {
            log.info("Using contact batch store {} in active version {}", contactTable.getName(), active);
            return contactTable;
        }

        return null;
    }

    /*
     * select one period table for each stream for calculating last activity date
     */
    private Map<String, List<String>> selectPeriodTables() {
        Map<String, String> periodStoreTableNames = getMapObjectFromContext(PERIOD_STORE_TABLE_NAME, String.class,
                String.class);

        Map<String, List<String>> selectedPeriodTableNames = new HashMap<>();
        configuration.getActivityStreamMap().forEach((streamId, stream) -> {
            if (CollectionUtils.isEmpty(stream.getPeriods())) {
                return;
            }

            // choose first one for now, TODO select least granular (less rows)
            stream.getPeriods().stream() //
                    .map(period -> {
                        String key = String.format(PERIOD_STORE_TABLE_FORMAT, streamId, period);
                        return periodStoreTableNames.get(key);
                    }) //
                    .filter(StringUtils::isNotBlank) //
                    .findFirst() //
                    .ifPresent(tableName -> {
                        String entity = getEntity(stream);
                        log.info("Selecting table {} for stream {}, entity={}", tableName, streamId, entity);
                        selectedPeriodTableNames.putIfAbsent(entity, new ArrayList<>());
                        selectedPeriodTableNames.get(entity).add(tableName);
                    });
        });
        return selectedPeriodTableNames;
    }

    /*-
     * get "leaf" entity (lowest one in hierarchy) of stream
     */
    private String getEntity(@NotNull AtlasStream stream) {
        if (stream.getMatchEntities().contains(Contact.name())) {
            return Contact.name();
        } else if (stream.getMatchEntities().contains(Account.name())) {
            return Account.name();
        } else {
            String msg = String.format(
                    "Stream %s does not have supported match entities (account/contact). Match entities: %s",
                    stream.getStreamId(), stream.getMatchEntities());
            throw new UnsupportedOperationException(msg);
        }
    }

    /*-
     * only execute when we are updating period store. better to keep in sync with metric steps
     */
    private boolean shouldExecute() {
        if (configuration.isShouldRebuild()) {
            log.info("In rebuild mode for activity store, re-calculating");
            return true;
        }
        Map<String, String> periodStoreTableNames = getMapObjectFromContext(PERIOD_STORE_TABLE_NAME, String.class,
                String.class);
        log.info("Period store table names = {}", periodStoreTableNames);
        return allTablesExist(periodStoreTableNames);
    }

    private boolean isShortCutMode() {
        Map<String, String> lastActivityDateTableNames = getMapObjectFromContext(LAST_ACTIVITY_DATE_TABLE_NAME,
                String.class, String.class);
        log.info("Last activity date table names = {}", lastActivityDateTableNames);
        return allTablesExist(lastActivityDateTableNames);
    }
}
