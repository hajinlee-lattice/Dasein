package com.latticeengines.cdl.workflow.steps.maintenance;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.config.atlas.PeriodCollectorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.atlas.PeriodDataCleanerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.atlas.PeriodDataDistributorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessTransactionStepConfiguration;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.serviceflows.workflow.util.TableCloneUtils;
import com.latticeengines.yarn.exposed.service.EMREnvService;

@Component(SoftDeleteTransaction.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class SoftDeleteTransaction extends BaseSingleEntitySoftDelete<ProcessTransactionStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(SoftDeleteTransaction.class);

    static final String BEAN_NAME = "softDeleteTransaction";

    @Inject
    private EMREnvService emrEnvService;

    private Table rawTable;

    @Override
    protected void initializeConfiguration() {
        super.initializeConfiguration();
        clonePeriodStore(batchStore);
        rawTable = dataCollectionProxy.getTable(customerSpace.toString(), //
                TableRoleInCollection.ConsolidatedRawTransaction, inactive);
    }

    @Override
    protected PipelineTransformationRequest getConsolidateRequest() {

        List<Action> actions = filterSoftDeleteActions(softDeleteActions, BusinessEntity.Account);
        if (CollectionUtils.isEmpty(actions)) {
            log.info("No suitable delete actions for Transaction. Skip this step.");
            return null;
        } else {
            PipelineTransformationRequest request = new PipelineTransformationRequest();
            request.setName("SoftDeleteTransaction");

            List<TransformationStepConfig> steps = new ArrayList<>();

            int softDeleteMergeStep = 0;
            int softDeleteStep = softDeleteMergeStep + 1;
            int collectRawStep = softDeleteStep + 1;
            int cleanupRawStep = collectRawStep + 1;
            int dayPeriodStep = cleanupRawStep + 1;

            TransformationStepConfig softDeleteMerge = mergeSoftDelete(actions, InterfaceName.AccountId.name());
            TransformationStepConfig softDelete = softDelete(softDeleteMergeStep);
            TransformationStepConfig collectRaw = collectRaw();
            TransformationStepConfig cleanupRaw = cleanupRaw(rawTable, collectRawStep);
            TransformationStepConfig dayPeriods = collectDays(softDeleteStep);
            TransformationStepConfig dailyPartition = partitionDaily(dayPeriodStep, softDeleteStep);
            steps.add(softDeleteMerge);
            steps.add(softDelete);
            steps.add(collectRaw);
            steps.add(cleanupRaw);
            steps.add(dayPeriods);
            steps.add(dailyPartition);

            request.setSteps(steps);
            return request;
        }
    }

    @Override
    protected boolean skipRegisterBatchStore() {
        return true;
    }

    @Override
    String getJoinColumn() {
        // delete by account id
        return InterfaceName.AccountId.name();
    }

    private TransformationStepConfig partitionDaily(int dayPeriodStep, int softDeleteStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_DATA_DISTRIBUTOR);
        List<Integer> inputSteps = new ArrayList<>();
        inputSteps.add(dayPeriodStep);
        inputSteps.add(softDeleteStep);
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
        config.setRetryable(false);
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        return step;
    }

    private TransformationStepConfig collectDays(int softDeleteStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_COLLECTOR);
        step.setInputSteps(Collections.singletonList(softDeleteStep));
        PeriodCollectorConfig config = new PeriodCollectorConfig();
        config.setPeriodField(InterfaceName.TransactionDayPeriod.name());
        step.setConfiguration(appendEngineConf(config, heavyMemoryEngineConfig()));
        return step;
    }

    private TransformationStepConfig cleanupRaw(Table periodTable, int collectRawStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_DATA_CLEANER);
        step.setInputSteps(Collections.singletonList(collectRawStep));

        String tableSourceName = "PeriodTable";
        String sourceTableName = periodTable.getName();
        SourceTable sourceTable = new SourceTable(sourceTableName, customerSpace);
        List<String> baseSources = Collections.singletonList(tableSourceName);
        step.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableSourceName, sourceTable);
        step.setBaseTables(baseTables);
        PeriodDataCleanerConfig config = new PeriodDataCleanerConfig();
        config.setPeriodField(InterfaceName.TransactionDayPeriod.name());
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        return step;
    }

    private TransformationStepConfig collectRaw() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_COLLECTOR);

        List<String> sourceNames = new ArrayList<>();
        Map<String, SourceTable> sourceTables = new HashMap<>();
        sourceNames.add(rawTable.getName());
        SourceTable sourceTable = new SourceTable(rawTable.getName(), customerSpace);
        sourceTables.put(rawTable.getName(), sourceTable);

        PeriodCollectorConfig config = new PeriodCollectorConfig();
        config.setPeriodField(InterfaceName.TransactionDayPeriod.name());

        step.setBaseSources(sourceNames);
        step.setBaseTables(sourceTables);
        step.setConfiguration(appendEngineConf(config, heavyMemoryEngineConfig()));
        return step;
    }

    private void clonePeriodStore(TableRoleInCollection role) {
        Table activeTable = dataCollectionProxy.getTable(customerSpace.toString(), role, active);
        String cloneName = NamingUtils.timestamp(role.name());
        String queue = LedpQueueAssigner.getPropDataQueueNameForSubmission();
        queue = LedpQueueAssigner.overwriteQueueAssignment(queue, emrEnvService.getYarnQueueScheme());
        Table inactiveTable = TableCloneUtils //
                .cloneDataTable(yarnConfiguration, customerSpace, cloneName, activeTable, queue);
        metadataProxy.createTable(customerSpace.toString(), cloneName, inactiveTable);
        dataCollectionProxy.upsertTable(customerSpace.toString(), cloneName, role, inactive);
    }
}
