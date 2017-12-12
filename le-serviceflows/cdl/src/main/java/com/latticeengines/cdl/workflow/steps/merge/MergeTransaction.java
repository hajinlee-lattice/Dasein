package com.latticeengines.cdl.workflow.steps.merge;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PeriodCollectorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PeriodDataDistributorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PeriodDateConvertorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessTransactionStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.serviceflows.workflow.util.TableCloneUtils;

@Component(MergeTransaction.BEAN_NAME)
public class MergeTransaction extends BaseMergeImports<ProcessTransactionStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(MergeTransaction.class);

    static final String BEAN_NAME = "mergeTransaction";

    private Table rawTable;
    private int mergeStep, dailyStep, dayPeriodStep;

    @Override
    protected TransformationWorkflowConfiguration executePreTransformation() {
        initializeConfiguration();
        return generateWorkflowConf();
    }

    @Override
    protected void onPostTransformationCompleted() {
        String diffTableName = TableUtils.getFullTableName(diffTablePrefix, pipelineVersion);
        updateEntityValueMapInContext(ENTITY_DIFF_TABLES, diffTableName, String.class);
        generateDiffReport();
    }

    protected void initializeConfiguration() {
        super.initializeConfiguration();
        initializePeriodStores();
        rawTable = dataCollectionProxy.getTable(customerSpace.toString(), //
                TableRoleInCollection.ConsolidatedRawTransaction, inactive);
        if (rawTable == null) {
            throw new IllegalStateException("Cannot find raw period store");
        }
        log.info("Found rawTable " + rawTable.getName());
    }

    private TransformationWorkflowConfiguration generateWorkflowConf() {
        PipelineTransformationRequest request = getConsolidateRequest();
        return transformationProxy.getWorkflowConf(request, configuration.getPodId());
    }

    public PipelineTransformationRequest getConsolidateRequest() {
        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName("MergeTransaction");

        mergeStep = 0;
        dailyStep = 1;
        dayPeriodStep = 2;

        TransformationStepConfig inputMerge = mergeInputs(false);
        TransformationStepConfig daily = addTrxDate();
        TransformationStepConfig dayPeriods  = collectDays();
        TransformationStepConfig dailyPartition  = partitionDaily();
        TransformationStepConfig report = reportDiff(dayPeriodStep);

        List<TransformationStepConfig> steps = new ArrayList<>();
        steps.add(inputMerge);
        steps.add(daily);
        steps.add(dayPeriods);
        steps.add(dailyPartition);
        steps.add(report);
        request.setSteps(steps);
        return request;
    }

    private TransformationStepConfig addTrxDate() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_DATE_CONVERTOR);
        step.setInputSteps(Collections.singletonList(mergeStep));
        PeriodDateConvertorConfig config = new PeriodDateConvertorConfig();
        config.setTrxTimeField(InterfaceName.TransactionTime.name());
        config.setTrxDateField(InterfaceName.TransactionDate.name());
        config.setTrxDayPeriodField(InterfaceName.TransactionDayPeriod.name());
        step.setConfiguration(JsonUtils.serialize(config));
        return step;
    }


    private TransformationStepConfig collectDays() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_COLLECTOR);
        step.setInputSteps(Collections.singletonList(dailyStep));
        PeriodCollectorConfig config = new PeriodCollectorConfig();
        config.setPeriodField(InterfaceName.TransactionDayPeriod.name());

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(diffTablePrefix);
        step.setTargetTable(targetTable);

        step.setConfiguration(JsonUtils.serialize(config));
        return step;
    }


    private TransformationStepConfig partitionDaily() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_DATA_DISTRIBUTOR);
        List<Integer> inputSteps = new ArrayList<Integer>();
        inputSteps.add(dayPeriodStep);
        inputSteps.add(dailyStep);
        step.setInputSteps(inputSteps);

        String tableSourceName = "RawTransaction";
        String sourceTableName = rawTable.getName();
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

    private void initializePeriodStores() {
        initOrClonePeriodStore(TableRoleInCollection.ConsolidatedRawTransaction, SchemaInterpretation.TransactionRaw);
        initOrClonePeriodStore(TableRoleInCollection.ConsolidatedDailyTransaction,
                SchemaInterpretation.TransactionDailyAggregation);
        initOrClonePeriodStore(TableRoleInCollection.ConsolidatedPeriodTransaction,
                SchemaInterpretation.TransactionPeriodAggregation);
    }


    private void initOrClonePeriodStore(TableRoleInCollection role, SchemaInterpretation schema) {
        String activeTableName = dataCollectionProxy.getTableName(customerSpace.toString(), role, active);
        if (StringUtils.isNotBlank(activeTableName)) {
            log.info("Cloning " + role + " from " + active + " to " + inactive);
            clonePeriodStore(role);
        } else {
            log.info("Building a brand new " + role);
            buildPeriodStore(role, schema);
        }
    }

    private void clonePeriodStore(TableRoleInCollection role) {
        log.info("Cloning " + role + " from  " + active + " to " + inactive);
        Table activeTable = dataCollectionProxy.getTable(customerSpace.toString(), role, active);
        String cloneName = NamingUtils.timestamp(role.name());
        String queue = LedpQueueAssigner.getPropDataQueueNameForSubmission();
        queue = LedpQueueAssigner.overwriteQueueAssignment(queue, queueScheme);
        Table inactiveTable = TableCloneUtils //
                .cloneDataTable(yarnConfiguration, customerSpace, cloneName, activeTable, queue);
        metadataProxy.createTable(customerSpace.toString(), cloneName, inactiveTable);
        dataCollectionProxy.upsertTable(customerSpace.toString(), cloneName, role, inactive);
    }

    private Table buildPeriodStore(TableRoleInCollection role, SchemaInterpretation schema) {
        Table table = SchemaRepository.instance().getSchema(schema);
        String hdfsPath = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), customerSpace, "").toString();

        try {
            log.info("Initialize period store " + hdfsPath + "/" + schema);
            HdfsUtils.mkdir(yarnConfiguration, hdfsPath + "/" + schema);
        } catch (Exception e) {
            log.error("Failed to initialize period store " + hdfsPath + "/" + schema);
            throw new RuntimeException("Failed to create period store " + role);
        }

        Extract extract = new Extract();
        extract.setName("extract_target");
        extract.setExtractionTimestamp(DateTime.now().getMillis());
        extract.setProcessedRecords(1L);
        extract.setPath(hdfsPath + "/" + schema + "/");
        table.setExtracts(Collections.singletonList(extract));
        metadataProxy.updateTable(customerSpace.toString(), table.getName(), table);
        dataCollectionProxy.upsertTable(customerSpace.toString(), table.getName(), role, inactive);
        log.info("Upsert table " + table.getName() + " to role " + role + "version" + inactive);

        return table;
    }

    @Override
    protected void cloneBatchStore() {
    }

}
