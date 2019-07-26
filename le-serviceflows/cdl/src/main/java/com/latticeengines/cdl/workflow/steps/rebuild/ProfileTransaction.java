package com.latticeengines.cdl.workflow.steps.rebuild;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_SORTER;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.cdl.ChoreographerContext;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PeriodCollectorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PeriodConvertorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PeriodDataAggregaterConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PeriodDataDistributorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.ProductMapperConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.SorterConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessTransactionStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.util.PeriodStrategyUtils;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.PeriodProxy;
import com.latticeengines.serviceflows.workflow.util.ScalingUtils;

@Component(ProfileTransaction.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ProfileTransaction extends ProfileStepBase<ProcessTransactionStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ProfileTransaction.class);

    public static final String BEAN_NAME = "profileTransaction";

    private DataCollection.Version inactive;
    private DataCollection.Version active;
    private int productAgrStep, periodedStep, dailyAgrStep, dayPeriodStep, periodAgrStep, periodsStep;
    private Table rawTable, dailyTable;
    private List<Table> periodTables;
    private Table productTable;

    private String sortedDailyTablePrefix;
    private String sortedPeriodTablePrefix;

    private String sortedDailyTableName;
    private String sortedPeriodTableName;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private PeriodProxy periodProxy;

    @Inject
    private Configuration yarnConfiguration;

    private List<PeriodStrategy> periodStrategies;

    private boolean entityMatchEnabled;

    private boolean shortCut = false;

    @Override
    protected BusinessEntity getEntity() {
        return BusinessEntity.PeriodTransaction;
    }

    private void initializeConfiguration() {
        customerSpace = configuration.getCustomerSpace();
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
        active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);

        List<Table> tablesInCtx = getTableSummariesFromCtxKeys(customerSpace.toString(), Arrays.asList(
                AGG_DAILY_TRXN_TABLE_NAME, AGG_PERIOD_TRXN_TABLE_NAME));
        shortCut = tablesInCtx.stream().noneMatch(Objects::isNull);

        if (shortCut) {

            log.info("Found both daily and period aggregated tables in context, going thru short-cut mode.");
            sortedDailyTableName = tablesInCtx.get(0).getName();
            sortedPeriodTableName = tablesInCtx.get(1).getName();
            finishing();

        } else {

            ChoreographerContext context = getObjectFromContext(CHOREOGRAPHER_CONTEXT_KEY, ChoreographerContext.class);
            boolean isBusinessCalendarChanged = context.isBusinessCalenderChanged();
            log.info("isBusinessCalendarChanged=" + isBusinessCalendarChanged);

            String rawTransactionTableName = getRawTransactionTableName();
            rawTable = metadataProxy.getTable(customerSpace.toString(), rawTransactionTableName);
            if (rawTable == null) {
                throw new RuntimeException("Cannot find raw transaction table.");
            }

            double sizeInGb = ScalingUtils.getTableSizeInGb(yarnConfiguration, rawTable);
            int multiplier = ScalingUtils.getMultiplier(sizeInGb);
            log.info("Set scalingMultiplier=" + multiplier + " base on master table size=" + sizeInGb + " gb.");
            scalingMultiplier = multiplier;

            periodStrategies = periodProxy.getPeriodStrategies(customerSpace.toString());

            buildPeriodStore(TableRoleInCollection.ConsolidatedDailyTransaction);
            buildPeriodStore(TableRoleInCollection.ConsolidatedPeriodTransaction);

            dailyTable = dataCollectionProxy.getTable(customerSpace.toString(),
                    TableRoleInCollection.ConsolidatedDailyTransaction, inactive);
            periodTables = dataCollectionProxy.getTables(customerSpace.toString(),
                    TableRoleInCollection.ConsolidatedPeriodTransaction, inactive);

            sortedDailyTablePrefix = TableRoleInCollection.AggregatedTransaction.name();
            sortedPeriodTablePrefix = TableRoleInCollection.AggregatedPeriodTransaction.name();

            getProductTable();

            entityMatchEnabled = configuration.isEntityMatchEnabled();
            if (entityMatchEnabled) {
                log.info("Entity match is enabled for transaction rebuild");
            }

        }
    }

    private String getRawTransactionTableName() {
        String rawTableName = dataCollectionProxy.getTableName(customerSpace.toString(),
                TableRoleInCollection.ConsolidatedRawTransaction, inactive);
        if (StringUtils.isBlank(rawTableName)) {
            rawTableName = dataCollectionProxy.getTableName(customerSpace.toString(),
                    TableRoleInCollection.ConsolidatedRawTransaction, active);
            if (StringUtils.isNotBlank(rawTableName)) {
                log.info("Found raw transaction table in active version " + active);
            }
        } else {
            log.info("Found raw transaction table in inactive version " + inactive);
        }
        return rawTableName;
    }

    private void buildPeriodStore(TableRoleInCollection role) {
        SchemaInterpretation schema;
        List<String> tablePrefixes = new ArrayList<>();
        switch (role) {
        case ConsolidatedDailyTransaction:
            schema = SchemaInterpretation.TransactionDailyAggregation;
            tablePrefixes.add("");
            break;
        case ConsolidatedPeriodTransaction:
            schema = SchemaInterpretation.TransactionPeriodAggregation;
            periodStrategies.forEach(strategy -> {
                tablePrefixes.add(PeriodStrategyUtils.getTablePrefixFromPeriodStrategy(strategy));
            });
            break;
        default:
            throw new UnsupportedOperationException(role + " is not a supported period store.");
        }

        List<String> tableNames = new ArrayList<>();
        for (String tablePrefix : tablePrefixes) {
            Table table = SchemaRepository.instance().getSchema(schema, false, entityMatchEnabled);
            String tableName = tablePrefix + NamingUtils.timestamp(role.name());
            table.setName(tableName);
            String hdfsPath = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), customerSpace, "")
                    .toString();

            try {
                log.info("Initialize period store " + hdfsPath + "/" + tableName);
                if (HdfsUtils.isDirectory(yarnConfiguration, hdfsPath + "/" + tableName)) {
                    HdfsUtils.rmdir(yarnConfiguration, hdfsPath + "/" + tableName);
                }
                HdfsUtils.mkdir(yarnConfiguration, hdfsPath + "/" + tableName);
            } catch (Exception e) {
                log.error("Failed to initialize period store " + hdfsPath + "/" + tableName);
                throw new RuntimeException("Failed to create period store " + role);
            }

            Extract extract = new Extract();
            extract.setName("extract_target");
            extract.setExtractionTimestamp(DateTime.now().getMillis());
            extract.setProcessedRecords(1L);
            extract.setPath(hdfsPath + "/" + tableName + "/*.avro");
            table.setExtracts(Collections.singletonList(extract));
            metadataProxy.updateTable(customerSpace.toString(), table.getName(), table);
            tableNames.add(table.getName());
        }

        dataCollectionProxy.upsertTables(customerSpace.toString(), tableNames, role, inactive);
        log.info("Upsert tables " + String.join(",", tableNames) + " to role=" + role + ", version=" + inactive);
    }

    @Override
    protected void onPostTransformationCompleted() {
        sortedDailyTableName = TableUtils.getFullTableName(sortedDailyTablePrefix, pipelineVersion);
        Table sortedDailyTable = metadataProxy.getTable(customerSpace.toString(), sortedDailyTableName);
        if (sortedDailyTable == null) {
            throw new IllegalStateException("sortedDailyTable is null.");
        }
        sortedDailyTableName = renameServingStoreTable(BusinessEntity.Transaction, sortedDailyTable);
        exportToS3AndAddToContext(sortedDailyTableName, AGG_DAILY_TRXN_TABLE_NAME);

        sortedPeriodTableName = TableUtils.getFullTableName(sortedPeriodTablePrefix, pipelineVersion);
        Table sortedPeriodTable = metadataProxy.getTable(customerSpace.toString(), sortedPeriodTableName);
        if (sortedPeriodTable == null) {
            throw new IllegalStateException("sortedPeriodTable is null.");
        }
        sortedPeriodTableName = renameServingStoreTable(BusinessEntity.PeriodTransaction, sortedPeriodTable);
        exportToS3AndAddToContext(sortedPeriodTableName, AGG_PERIOD_TRXN_TABLE_NAME);

        finishing();
    }

    private void finishing() {
        dataCollectionProxy.upsertTable(customerSpace.toString(), sortedDailyTableName,
                BusinessEntity.Transaction.getServingStore(), inactive);
        exportTableRoleToRedshift(sortedDailyTableName, BusinessEntity.Transaction.getServingStore());

        dataCollectionProxy.upsertTable(customerSpace.toString(), sortedPeriodTableName,
                BusinessEntity.PeriodTransaction.getServingStore(), inactive);
        exportTableRoleToRedshift(sortedPeriodTableName, BusinessEntity.PeriodTransaction.getServingStore());
    }

    @Override
    protected TransformationWorkflowConfiguration executePreTransformation() {
        initializeConfiguration();

        if (shortCut) {
            return null;
        }

        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName("ProfileTransaction");
        request.setSubmitter(customerSpace.getTenantId());
        request.setKeepTemp(false);
        request.setEnableSlack(false);
        List<TransformationStepConfig> steps = new ArrayList<>();

        productAgrStep = 0;
        periodedStep = 1;
        dailyAgrStep = 2;
        dayPeriodStep = 3;
        TransformationStepConfig productAgr = rollupProduct(productTable); // productAgrStep
        TransformationStepConfig perioded = addPeriod(productAgrStep, null); // periodedStep
        TransformationStepConfig dailyAgr = aggregateDaily(); // dailyAgrStep
        TransformationStepConfig dayPeriods = collectDays(); // dayPeriodStep
        TransformationStepConfig updateDaily = updateDailyStore();
        steps.add(productAgr); // step 0
        steps.add(perioded); // step 1
        steps.add(dailyAgr); // step 2
        steps.add(dayPeriods); // step 3
        steps.add(updateDaily); // step 4

        periodedStep = 5;
        periodAgrStep = 6;
        periodsStep = 7;
        perioded = addPeriod(dailyAgrStep, periodStrategies); // periodedStep
        TransformationStepConfig periodAgr = aggregatePeriods(); // periodAgrStep
        TransformationStepConfig periods = collectPeriods(); // periodsStep
        TransformationStepConfig updatePeriod = updatePeriodStore(periodTables);
        TransformationStepConfig sortDaily = sort(dailyTable.getName(), null, sortedDailyTablePrefix);
        TransformationStepConfig sortPeriod = sort(null, periodAgrStep, sortedPeriodTablePrefix);
        steps.add(perioded); // step 5
        steps.add(periodAgr); // step 6
        steps.add(periods); // step 7
        steps.add(updatePeriod); // step 8
        steps.add(sortDaily); // step 9
        steps.add(sortPeriod); // step 10

        request.setSteps(steps);
        return transformationProxy.getWorkflowConf(customerSpace.toString(), request, configuration.getPodId());
    }

    private void getProductTable() {
        productTable = dataCollectionProxy.getTable(customerSpace.toString(),
                TableRoleInCollection.ConsolidatedProduct, inactive);
        if (productTable == null) {
            log.info("Did not find product table in inactive version.");
            productTable = dataCollectionProxy.getTable(customerSpace.toString(),
                    TableRoleInCollection.ConsolidatedProduct, active);
            if (productTable == null) {
                throw new IllegalStateException("Cannot find the product table in both versions");
            }
        }
        log.info(String.format("productTableName for customer %s is %s",
                configuration.getCustomerSpace().toString(), productTable.getName()));
    }

    private TransformationStepConfig rollupProduct(Table productTable) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PRODUCT_MAPPER);
        String tableSourceName = "CustomerUniverse";
        String sourceTableName = rawTable.getName();
        SourceTable sourceTable = new SourceTable(sourceTableName, customerSpace);
        String productTableName = productTable.getName();
        List<String> baseSources = Arrays.asList(tableSourceName, productTableName);
        step.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableSourceName, sourceTable);
        baseTables.put(productTableName, new SourceTable(productTableName, customerSpace));
        step.setBaseTables(baseTables);

        ProductMapperConfig config = new ProductMapperConfig();
        config.setProductField(InterfaceName.ProductId.name());
        config.setProductTypeField(InterfaceName.ProductType.name());

        step.setConfiguration(JsonUtils.serialize(config));
        return step;
    }

    private TransformationStepConfig addPeriod(int inputStep, List<PeriodStrategy> periodStrategies) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_CONVERTOR);
        step.setInputSteps(Collections.singletonList(inputStep));
        PeriodConvertorConfig config = new PeriodConvertorConfig();
        config.setTrxDateField(InterfaceName.TransactionDate.name());
        config.setPeriodStrategies(periodStrategies);
        config.setPeriodField(InterfaceName.PeriodId.name());
        step.setConfiguration(JsonUtils.serialize(config));
        return step;
    }

    private TransformationStepConfig aggregateDaily() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_DATA_AGGREGATER);
        step.setInputSteps(Collections.singletonList(periodedStep));
        PeriodDataAggregaterConfig config = new PeriodDataAggregaterConfig();
        config.setSumFields(
                Arrays.asList(InterfaceName.Amount.name(), InterfaceName.Cost.name(), InterfaceName.Quantity.name()));
        config.setSumOutputFields(Arrays.asList(InterfaceName.TotalAmount.name(), InterfaceName.TotalCost.name(),
                InterfaceName.TotalQuantity.name()));
        config.setCountField(Collections.singletonList(InterfaceName.TransactionTime.name()));
        config.setCountOutputField(Collections.singletonList(InterfaceName.TransactionCount.name()));
        List<String> groupByFields = new ArrayList<>();
        if (entityMatchEnabled) {
            // In the future, Transaction could have more account fields, need
            // to consider:
            // 1. Are they needed in transaction store
            // 2. How to properly and efficiently retain them -- Keeping adding
            // in group fields could have performance concern; Add a join?
            groupByFields.add(InterfaceName.CustomerAccountId.name());
        }
        groupByFields.addAll(Arrays.asList( //
                InterfaceName.AccountId.name(), //
                InterfaceName.ProductId.name(), //
                InterfaceName.ProductType.name(), //
                InterfaceName.TransactionType.name(), //
                InterfaceName.TransactionDate.name(), //
                InterfaceName.TransactionDayPeriod.name()));
        config.setGroupByFields(groupByFields);
        step.setConfiguration(JsonUtils.serialize(config));
        return step;
    }

    private TransformationStepConfig collectDays() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_COLLECTOR);
        step.setInputSteps(Collections.singletonList(dailyAgrStep));
        PeriodCollectorConfig config = new PeriodCollectorConfig();
        config.setPeriodField(InterfaceName.TransactionDayPeriod.name());
        step.setConfiguration(JsonUtils.serialize(config));
        return step;
    }

    private TransformationStepConfig updateDailyStore() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_DATA_DISTRIBUTOR);
        List<Integer> inputSteps = new ArrayList<>();
        inputSteps.add(dayPeriodStep);
        inputSteps.add(dailyAgrStep);
        step.setInputSteps(inputSteps);

        String tableSourceName = "CustomerUniverse";
        String sourceTableName = dailyTable.getName();
        SourceTable sourceTable = new SourceTable(sourceTableName, customerSpace);
        List<String> baseSources = Collections.singletonList(tableSourceName);
        step.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableSourceName, sourceTable);
        step.setBaseTables(baseTables);

        PeriodDataDistributorConfig config = new PeriodDataDistributorConfig();
        config.setPeriodField(InterfaceName.TransactionDayPeriod.name());
        step.setConfiguration(JsonUtils.serialize(config));
        return step;
    }

    private TransformationStepConfig aggregatePeriods() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_DATA_AGGREGATER);
        step.setInputSteps(Collections.singletonList(periodedStep));
        PeriodDataAggregaterConfig config = new PeriodDataAggregaterConfig();
        config.setSumFields(Arrays.asList(InterfaceName.TotalAmount.name(), InterfaceName.TotalCost.name(),
                InterfaceName.TransactionCount.name(), InterfaceName.TotalQuantity.name()));
        config.setSumOutputFields(Arrays.asList(InterfaceName.TotalAmount.name(), InterfaceName.TotalCost.name(),
                InterfaceName.TransactionCount.name(), InterfaceName.TotalQuantity.name()));
        List<String> groupByFields = new ArrayList<>();
        if (entityMatchEnabled) {
            // In the future, Transaction could have more account fields, need
            // to consider:
            // 1. Are they needed in transaction store
            // 2. How to properly and efficiently retain them -- Keeping adding
            // in group fields could have performance concern; Add a join?
            groupByFields.add(InterfaceName.CustomerAccountId.name());
        }
        groupByFields.addAll(Arrays.asList( //
                InterfaceName.AccountId.name(), //
                InterfaceName.ProductId.name(), //
                InterfaceName.ProductType.name(), //
                InterfaceName.TransactionType.name(), //
                InterfaceName.PeriodId.name(), //
                InterfaceName.PeriodName.name()));
        config.setGroupByFields(groupByFields);
        step.setConfiguration(JsonUtils.serialize(config));
        return step;
    }

    private TransformationStepConfig collectPeriods() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_COLLECTOR);
        step.setInputSteps(Collections.singletonList(periodAgrStep));
        PeriodCollectorConfig config = new PeriodCollectorConfig();
        config.setPeriodField(InterfaceName.PeriodId.name());
        config.setPeriodNameField(InterfaceName.PeriodName.name());
        step.setConfiguration(JsonUtils.serialize(config));
        return step;
    }

    private TransformationStepConfig updatePeriodStore(List<Table> periodTables) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_DATA_DISTRIBUTOR);
        List<Integer> inputSteps = new ArrayList<>();
        inputSteps.add(periodsStep);
        inputSteps.add(periodAgrStep);
        step.setInputSteps(inputSteps);

        List<String> baseSources = new ArrayList<>();
        Map<String, SourceTable> baseTables = new HashMap<>();
        Map<String, Integer> transactionIdxes = new HashMap<>();
        for (int i = 0; i < periodTables.size(); i++) {
            Table periodTable = periodTables.get(i);
            String tableSourceName = "PeriodTable" + periodTable.getName();
            String sourceTableName = periodTable.getName();
            SourceTable sourceTable = new SourceTable(sourceTableName, customerSpace);
            baseSources.add(tableSourceName);
            baseTables.put(tableSourceName, sourceTable);
            transactionIdxes.put(PeriodStrategyUtils.getPeriodStrategyNameFromPeriodTableName(periodTable.getName(),
                    TableRoleInCollection.ConsolidatedPeriodTransaction), i + 2);
        }
        step.setBaseSources(baseSources);
        step.setBaseTables(baseTables);

        PeriodDataDistributorConfig config = new PeriodDataDistributorConfig();
        config.setMultiPeriod(true);
        config.setPeriodField(InterfaceName.PeriodId.name());
        config.setPeriodNameField(InterfaceName.PeriodName.name());
        config.setTransactionIdxes(transactionIdxes);
        step.setConfiguration(JsonUtils.serialize(config));
        return step;
    }

    private TransformationStepConfig sort(String sourceTableName, Integer inputStep, String prefix) {
        if (sourceTableName != null && inputStep != null) {
            throw new RuntimeException(TRANSFORMER_SORTER + " can only sort one base table");
        }
        TransformationStepConfig step = new TransformationStepConfig();
        if (sourceTableName != null) {
            String tableSourceName = "CustomerUniverse";
            SourceTable sourceTable = new SourceTable(sourceTableName, customerSpace);
            List<String> baseSources = Collections.singletonList(tableSourceName);
            step.setBaseSources(baseSources);
            Map<String, SourceTable> baseTables = new HashMap<>();
            baseTables.put(tableSourceName, sourceTable);
            step.setBaseTables(baseTables);
        }
        if (inputStep != null) {
            List<Integer> inputSteps = Collections.singletonList(inputStep);
            step.setInputSteps(inputSteps);
        }
        step.setTransformer(TRANSFORMER_SORTER);

        SorterConfig config = new SorterConfig();
        config.setPartitions(50);
        String sortingKey = InterfaceName.AccountId.name();
        config.setSortingField(sortingKey);
        config.setCompressResult(true);
        step.setConfiguration(appendEngineConf(config, extraHeavyEngineConfig()));

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(prefix);
        targetTable.setPrimaryKey(InterfaceName.__Composite_Key__.name());
        step.setTargetTable(targetTable);

        return step;
    }
}
