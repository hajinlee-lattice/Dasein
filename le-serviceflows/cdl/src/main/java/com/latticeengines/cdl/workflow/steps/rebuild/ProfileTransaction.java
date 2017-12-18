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
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.cdl.workflow.steps.ProfileStepBase;
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
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.domain.exposed.util.TimeSeriesUtils;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;

@Component(ProfileTransaction.BEAN_NAME)
public class ProfileTransaction extends ProfileStepBase<ProcessTransactionStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ProfileTransaction.class);

    public static final String BEAN_NAME = "profileTransaction";

    private CustomerSpace customerSpace;
    private DataCollection.Version inactive;
    private DataCollection.Version active;
    private int productAgrStep, periodedStep, dailyAgrStep, dayPeriodStep, periodAgrStep, periodsStep;
    private Table rawTable, dailyTable, periodTable;
    private Map<String, Product> productMap;

    private String sortedDailyTablePrefix;
    private String sortedPeriodTablePrefix;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private YarnConfiguration yarnConfiguration;

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

        buildPeriodStore(TableRoleInCollection.ConsolidatedDailyTransaction);
        buildPeriodStore(TableRoleInCollection.ConsolidatedPeriodTransaction);
        dailyTable = dataCollectionProxy.getTable(customerSpace.toString(),
                TableRoleInCollection.ConsolidatedDailyTransaction, inactive);
        periodTable = dataCollectionProxy.getTable(customerSpace.toString(),
                TableRoleInCollection.ConsolidatedPeriodTransaction, inactive);

        sortedDailyTablePrefix = TableRoleInCollection.AggregatedTransaction.name();
        sortedPeriodTablePrefix = TableRoleInCollection.AggregatedPeriodTransaction.name();

        loadProductMap();
    }

    private String getRawTableName() {
        String rawTableName =  dataCollectionProxy.getTableName(customerSpace.toString(),
                TableRoleInCollection.ConsolidatedRawTransaction, inactive);
        if (StringUtils.isBlank(rawTableName)) {
            rawTableName =  dataCollectionProxy.getTableName(customerSpace.toString(),
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
        switch (role) {
            case ConsolidatedDailyTransaction:
                schema = SchemaInterpretation.TransactionDailyAggregation;
                break;
            case ConsolidatedPeriodTransaction:
                schema = SchemaInterpretation.TransactionPeriodAggregation;
                break;
            default:
                throw new UnsupportedOperationException(role + " is not a supported period store.");
        }

        Table table = SchemaRepository.instance().getSchema(schema);
        String tableName = NamingUtils.timestamp(role.name());
        table.setName(tableName);
        String hdfsPath = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), customerSpace, "").toString();

        try {
            log.info("Initialize period store " + hdfsPath + "/" + tableName);
            HdfsUtils.mkdir(yarnConfiguration, hdfsPath + "/" + tableName);
        } catch (Exception e) {
            log.error("Failed to initialize period store " + hdfsPath + "/" + tableName);
            throw new RuntimeException("Failed to create period store " + role);
        }

        Extract extract = new Extract();
        extract.setName("extract_target");
        extract.setExtractionTimestamp(DateTime.now().getMillis());
        extract.setProcessedRecords(1L);
        extract.setPath(hdfsPath + "/" + tableName + "/");
        table.setExtracts(Collections.singletonList(extract));
        metadataProxy.updateTable(customerSpace.toString(), table.getName(), table);
        dataCollectionProxy.upsertTable(customerSpace.toString(), table.getName(), role, inactive);
        log.info("Upsert table " + table.getName() + " to role " + role + "version" + inactive);
    }

    @Override
    protected void onPostTransformationCompleted() {
        String sortedDailyTableName = TableUtils.getFullTableName(sortedDailyTablePrefix, pipelineVersion);
        String sortedPeriodTableName = TableUtils.getFullTableName(sortedPeriodTablePrefix, pipelineVersion);
        putObjectInContext(DAILY_AGG_TXN_TABLE_NAME, sortedDailyTableName);
        putObjectInContext(PERIOD_AGG_TXN_TABLE_NAME, sortedPeriodTableName);
        updateEntityValueMapInContext(BusinessEntity.Transaction, TABLE_GOING_TO_REDSHIFT, sortedDailyTableName, String.class);
        updateEntityValueMapInContext(BusinessEntity.Transaction, APPEND_TO_REDSHIFT_TABLE, false, Boolean.class);
        updateEntityValueMapInContext(BusinessEntity.PeriodTransaction, TABLE_GOING_TO_REDSHIFT, sortedPeriodTableName, String.class);
        updateEntityValueMapInContext(BusinessEntity.PeriodTransaction, APPEND_TO_REDSHIFT_TABLE, false, Boolean.class);
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
        periodAgrStep = 5;
        periodsStep = 6;

        TransformationStepConfig productAgr = rollupProduct(productMap);
        TransformationStepConfig perioded = addPeriod();
        TransformationStepConfig dailyAgr = aggregateDaily();
        TransformationStepConfig dayPeriods = collectDays();
        TransformationStepConfig updateDaily = updateDailyStore();
        TransformationStepConfig periodAgr = aggregatePeriods();
        TransformationStepConfig periods = collectPeriods();
        TransformationStepConfig updatePeriod = updatePeriodStore();
        TransformationStepConfig sortDaily = sort(customerSpace, dailyTable, sortedDailyTablePrefix);
        TransformationStepConfig sortPeriod = sort(customerSpace, periodTable, sortedPeriodTablePrefix);
        steps.add(productAgr);
        steps.add(perioded);
        steps.add(dailyAgr);
        steps.add(dayPeriods);
        steps.add(updateDaily);
        steps.add(periodAgr);
        steps.add(periods);
        steps.add(updatePeriod);
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
        productMap = TimeSeriesUtils.loadProductMap(yarnConfiguration, productTable);
    }

    private TransformationStepConfig rollupProduct(Map<String, Product> productMap) {
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
        config.setProductMap(productMap);

        step.setConfiguration(JsonUtils.serialize(config));
        return step;
    }

    private TransformationStepConfig addPeriod() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_CONVERTOR);
        step.setInputSteps(Collections.singletonList(productAgrStep));
        PeriodConvertorConfig config = new PeriodConvertorConfig();
        config.setTrxDateField(InterfaceName.TransactionDate.name());
        config.setPeriodStrategy(PeriodStrategy.CalendarMonth);
        config.setPeriodField(InterfaceName.PeriodId.name());
        step.setConfiguration(JsonUtils.serialize(config));
        return step;
    }

    private TransformationStepConfig aggregateDaily() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_DATA_AGGREGATER);
        step.setInputSteps(Collections.singletonList(periodedStep));
        PeriodDataAggregaterConfig config = new PeriodDataAggregaterConfig();
        config.setSumFields(Collections.singletonList("Amount"));
        config.setSumOutputFields(Collections.singletonList("TotalAmount"));
        config.setSumLongFields(Collections.singletonList("Quantity"));
        config.setSumLongOutputFields(Collections.singletonList("TotalQuantity"));
        config.setGoupByFields(Arrays.asList(
                InterfaceName.AccountId.name(), //
                InterfaceName.ContactId.name(), //
                InterfaceName.ProductId.name(), //
                InterfaceName.TransactionType.name(), //
                InterfaceName.TransactionDate.name(), //
                InterfaceName.PeriodId.name(), //
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
        List<Integer> inputSteps = new ArrayList<Integer>();
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
        step.setInputSteps(Collections.singletonList(dailyAgrStep));
        PeriodDataAggregaterConfig config = new PeriodDataAggregaterConfig();
        config.setSumFields(Collections.singletonList("TotalAmount"));
        config.setSumOutputFields(Collections.singletonList("TotalAmount"));
        config.setSumLongFields(Collections.singletonList("TotalQuantity"));
        config.setSumLongOutputFields(Collections.singletonList("TotalQuantity"));
        config.setGoupByFields(Arrays.asList(
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

    private TransformationStepConfig updatePeriodStore() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_DATA_DISTRIBUTOR);
        List<Integer> inputSteps = new ArrayList<Integer>();
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

    private TransformationStepConfig sort(CustomerSpace customerSpace, Table table, String prefix) {
        TransformationStepConfig step = new TransformationStepConfig();
        String tableSourceName = "CustomerUniverse";
        SourceTable sourceTable = new SourceTable(table.getName(), customerSpace);
        List<String> baseSources = Collections.singletonList(tableSourceName);
        step.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableSourceName, sourceTable);
        step.setBaseTables(baseTables);
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
