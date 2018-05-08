package com.latticeengines.cdl.workflow.steps.rebuild;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_SORTER;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import com.latticeengines.cdl.utils.PeriodStrategyUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PeriodCollectorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PeriodConvertorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PeriodDataAggregaterConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PeriodDataDistributorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ProductMapperConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.SorterConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.metadata.transaction.Product;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessTransactionStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.util.ProductUtils;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.PeriodProxy;

@Component(ProfileTransaction.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ProfileTransaction extends ProfileStepBase<ProcessTransactionStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ProfileTransaction.class);

    public static final String BEAN_NAME = "profileTransaction";

    private CustomerSpace customerSpace;
    private DataCollection.Version inactive;
    private DataCollection.Version active;
    private int productAgrStep, periodedStep, dailyAgrStep, dayPeriodStep, periodAgrStep, periodsStep, mergePeriodStep;
    private Table rawTable, dailyTable;
    private List<Table> periodTables;
    private Map<String, List<Product>> productMap;

    private String sortedDailyTablePrefix;
    private String sortedPeriodTablePrefix;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private PeriodProxy periodProxy;

    @Inject
    private Configuration yarnConfiguration;

    private List<PeriodStrategy> periodStrategies;

    @Override
    protected BusinessEntity getEntity() {
        return BusinessEntity.PeriodTransaction;
    }

    private void initializeConfiguration() {
        customerSpace = configuration.getCustomerSpace();
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
        active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);

        String rawTableName = getRawTableName();
        rawTable = metadataProxy.getTable(customerSpace.toString(), rawTableName);
        if (rawTable == null) {
            throw new RuntimeException("Cannot find raw transaction table.");
        }

        periodStrategies = periodProxy.getPeriodStrategies(customerSpace.toString());

        buildPeriodStore(TableRoleInCollection.ConsolidatedDailyTransaction);
        buildPeriodStore(TableRoleInCollection.ConsolidatedPeriodTransaction);

        dailyTable = dataCollectionProxy.getTable(customerSpace.toString(),
                TableRoleInCollection.ConsolidatedDailyTransaction, inactive);
        periodTables = dataCollectionProxy.getTables(customerSpace.toString(),
                TableRoleInCollection.ConsolidatedPeriodTransaction, inactive);

        sortedDailyTablePrefix = TableRoleInCollection.AggregatedTransaction.name();
        sortedPeriodTablePrefix = TableRoleInCollection.AggregatedPeriodTransaction.name();

        loadProductMap();
    }

    private String getRawTableName() {
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
            Table table = SchemaRepository.instance().getSchema(schema);
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
        log.info("Upsert tables " + String.join(",", tableNames) + " to role " + role + "version" + inactive);
    }

    @Override
    protected void onPostTransformationCompleted() {
        String sortedDailyTableName = TableUtils.getFullTableName(sortedDailyTablePrefix, pipelineVersion);
        Table sortedDailyTable = metadataProxy.getTable(customerSpace.toString(), sortedDailyTableName);
        if (sortedDailyTable == null) {
            throw new IllegalStateException("sortedDailyTable is null.");
        }
        sortedDailyTableName = renameServingStoreTable(BusinessEntity.Transaction, sortedDailyTable);
        exportTableRoleToRedshift(sortedDailyTableName, BusinessEntity.Transaction.getServingStore());
        dataCollectionProxy.upsertTable(customerSpace.toString(), sortedDailyTableName,
                BusinessEntity.Transaction.getServingStore(), inactive);

        String sortedPeriodTableName = TableUtils.getFullTableName(sortedPeriodTablePrefix, pipelineVersion);
        Table sortedPeriodTable = metadataProxy.getTable(customerSpace.toString(), sortedPeriodTableName);
        if (sortedPeriodTable == null) {
            throw new IllegalStateException("sortedPeriodTable is null.");
        }
        sortedPeriodTableName = renameServingStoreTable(BusinessEntity.PeriodTransaction, sortedPeriodTable);
        exportTableRoleToRedshift(sortedPeriodTableName, BusinessEntity.PeriodTransaction.getServingStore());
        dataCollectionProxy.upsertTable(customerSpace.toString(), sortedDailyTableName,
                BusinessEntity.Transaction.getServingStore(), inactive);
    }

    @Override
    protected TransformationWorkflowConfiguration executePreTransformation() {
        initializeConfiguration();

        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName("CalculatePurchaseHistory");
        request.setSubmitter(customerSpace.getTenantId());
        request.setKeepTemp(false);
        request.setEnableSlack(false);
        List<TransformationStepConfig> steps = new ArrayList<>();

        productAgrStep = 0;
        periodedStep = 1;
        dailyAgrStep = 2;
        dayPeriodStep = 3;

        TransformationStepConfig productAgr = rollupProduct(productMap); // productAgrStep
        TransformationStepConfig perioded = addPeriod(productAgrStep, null); // periodedStep
        TransformationStepConfig dailyAgr = aggregateDaily(); // dailyAgrStep
        TransformationStepConfig dayPeriods = collectDays(); // dayPeriodStep
        TransformationStepConfig updateDaily = updateDailyStore();
        steps.add(productAgr);
        steps.add(perioded);
        steps.add(dailyAgr);
        steps.add(dayPeriods);
        steps.add(updateDaily);

        periodedStep = 5;
        periodAgrStep = 6;
        periodsStep = 6;
        mergePeriodStep = 5; // will be updated in strategy loop
        for (PeriodStrategy strategy : periodStrategies) {
            perioded = addPeriod(dailyAgrStep, strategy); // periodedStep
            TransformationStepConfig periodAgr = aggregatePeriods(strategy); // periodAgrStep
            TransformationStepConfig periods = collectPeriods(); // periodsStep
            TransformationStepConfig updatePeriod = updatePeriodStore(
                    PeriodStrategyUtils.findPeriodTableFromStrategy(periodTables, strategy));

            steps.add(perioded);
            steps.add(periodAgr);
            steps.add(periods);
            steps.add(updatePeriod);

            periodedStep += 4;
            periodAgrStep += 4;
            periodsStep += 4;
            mergePeriodStep += 4;
        }

        TransformationStepConfig mergePeriod = mergePeriodStore(); // mergePeriodStep
        TransformationStepConfig sortDaily = sort(dailyTable.getName(), null, sortedDailyTablePrefix);
        TransformationStepConfig sortPeriod = sort(null, mergePeriodStep, sortedPeriodTablePrefix);
        steps.add(mergePeriod);
        steps.add(sortDaily);
        steps.add(sortPeriod);

        request.setSteps(steps);
        return transformationProxy.getWorkflowConf(request, configuration.getPodId());
    }

    private void loadProductMap() {
        Table productTable = dataCollectionProxy.getTable(customerSpace.toString(),
                TableRoleInCollection.ConsolidatedProduct, inactive);
        if (productTable == null) {
            log.info("Did not find product table in inactive version.");
            productTable = dataCollectionProxy.getTable(customerSpace.toString(),
                    TableRoleInCollection.ConsolidatedProduct, active);
            if (productTable == null) {
                throw new IllegalStateException("Cannot find the product table in both versions");
            }
        }
        log.info(String.format("productTableName for customer %s is %s", configuration.getCustomerSpace().toString(),
                productTable.getName()));

        List<Product> productList = new ArrayList<>();
        productTable.getExtracts().forEach(
                extract -> productList.addAll(ProductUtils.loadProducts(yarnConfiguration, extract.getPath())));
        productMap = ProductUtils.getActiveProductMap(productList);
    }

    private TransformationStepConfig rollupProduct(Map<String, List<Product>> productMap) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PRODUCT_MAPPER);
        String tableSourceName = "CustomerUniverse";
        String sourceTableName = rawTable.getName();
        SourceTable sourceTable = new SourceTable(sourceTableName, customerSpace);
        List<String> baseSources = Collections.singletonList(tableSourceName);
        step.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableSourceName, sourceTable);
        step.setBaseTables(baseTables);

        ProductMapperConfig config = new ProductMapperConfig();
        config.setProductField(InterfaceName.ProductId.name());
        config.setProductTypeField(InterfaceName.ProductType.name());
        config.setProductMap(productMap);

        step.setConfiguration(JsonUtils.serialize(config));
        return step;
    }

    private TransformationStepConfig addPeriod(int inputStep, PeriodStrategy strategy) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_CONVERTOR);
        step.setInputSteps(Collections.singletonList(inputStep));
        PeriodConvertorConfig config = new PeriodConvertorConfig();
        config.setTrxDateField(InterfaceName.TransactionDate.name());
        config.setPeriodStrategy(strategy);
        config.setPeriodField(InterfaceName.PeriodId.name());
        step.setConfiguration(JsonUtils.serialize(config));
        return step;
    }

    private TransformationStepConfig aggregateDaily() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_DATA_AGGREGATER);
        step.setInputSteps(Collections.singletonList(periodedStep));
        PeriodDataAggregaterConfig config = new PeriodDataAggregaterConfig();
        config.setSumFields(Arrays.asList(InterfaceName.Amount.name(), InterfaceName.Cost.name()));
        config.setSumOutputFields(Arrays.asList(InterfaceName.TotalAmount.name(), InterfaceName.TotalCost.name()));
        config.setSumLongFields(Collections.singletonList(InterfaceName.Quantity.name()));
        config.setSumLongOutputFields(Collections.singletonList(InterfaceName.TotalQuantity.name()));
        config.setCountField(Collections.singletonList(InterfaceName.TransactionTime.name()));
        config.setCountOutputField(Collections.singletonList(InterfaceName.TransactionCount.name()));
        config.setGroupByFields(Arrays.asList( //
                InterfaceName.AccountId.name(), //
                InterfaceName.ContactId.name(), //
                InterfaceName.ProductId.name(), //
                InterfaceName.TransactionType.name(), //
                InterfaceName.TransactionDate.name(), //
                InterfaceName.TransactionDayPeriod.name()));
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

    private TransformationStepConfig aggregatePeriods(PeriodStrategy strategy) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_DATA_AGGREGATER);
        step.setInputSteps(Collections.singletonList(periodedStep));
        PeriodDataAggregaterConfig config = new PeriodDataAggregaterConfig();
        config.setSumFields(Arrays.asList(InterfaceName.TotalAmount.name(), InterfaceName.TotalCost.name(),
                InterfaceName.TransactionCount.name()));
        config.setSumOutputFields(Arrays.asList(InterfaceName.TotalAmount.name(), InterfaceName.TotalCost.name(),
                InterfaceName.TransactionCount.name()));
        config.setSumLongFields(Collections.singletonList(InterfaceName.TotalQuantity.name()));
        config.setSumLongOutputFields(Collections.singletonList(InterfaceName.TotalQuantity.name()));
        config.setPeriodStrategy(strategy);
        config.setGroupByFields(Arrays.asList( //
                InterfaceName.AccountId.name(), //
                InterfaceName.ContactId.name(), //
                InterfaceName.ProductId.name(), //
                InterfaceName.TransactionType.name(), //
                InterfaceName.PeriodId.name()));
        step.setConfiguration(JsonUtils.serialize(config));
        return step;
    }

    private TransformationStepConfig collectPeriods() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_COLLECTOR);
        step.setInputSteps(Collections.singletonList(periodAgrStep));
        PeriodCollectorConfig config = new PeriodCollectorConfig();
        config.setPeriodField(InterfaceName.PeriodId.name());
        step.setConfiguration(JsonUtils.serialize(config));
        return step;
    }

    private TransformationStepConfig updatePeriodStore(Table periodTable) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_DATA_DISTRIBUTOR);
        List<Integer> inputSteps = new ArrayList<>();
        inputSteps.add(periodsStep);
        inputSteps.add(periodAgrStep);
        step.setInputSteps(inputSteps);

        String tableSourceName = "CustomerUniverse";
        String sourceTableName = periodTable.getName();
        SourceTable sourceTable = new SourceTable(sourceTableName, customerSpace);
        List<String> baseSources = Collections.singletonList(tableSourceName);
        step.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableSourceName, sourceTable);
        step.setBaseTables(baseTables);

        PeriodDataDistributorConfig config = new PeriodDataDistributorConfig();
        config.setPeriodField(InterfaceName.PeriodId.name());
        step.setConfiguration(JsonUtils.serialize(config));
        return step;
    }

    private TransformationStepConfig mergePeriodStore() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.TRANSFORMER_MERGE);

        List<String> baseSources = new ArrayList<>();
        Map<String, SourceTable> baseTables = new HashMap<>();
        for (Table periodTable : periodTables) {
            String tableSourceName = periodTable.getName();
            String sourceTableName = periodTable.getName();
            SourceTable sourceTable = new SourceTable(sourceTableName, customerSpace);
            baseSources.add(tableSourceName);
            baseTables.put(tableSourceName, sourceTable);
        }
        step.setBaseSources(baseSources);
        step.setBaseTables(baseTables);

        step.setConfiguration(emptyStepConfig(lightEngineConfig()));
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
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(prefix);
        targetTable.setPrimaryKey(InterfaceName.__Composite_Key__.name());
        step.setTargetTable(targetTable);

        return step;
    }
}
