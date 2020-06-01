package com.latticeengines.cdl.workflow.steps.maintenance;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
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
import com.latticeengines.domain.exposed.pls.DeleteActionConfiguration;
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

    // global time ranges to apply to delete time series data
    private Set<List<Long>> deleteTimeRanges = new HashSet<>();

    @Override
    protected void initializeConfiguration() {
        super.initializeConfiguration();
        clonePeriodStore(batchStore);
        rawTable = dataCollectionProxy.getTable(customerSpace.toString(), //
                TableRoleInCollection.ConsolidatedRawTransaction, inactive);
    }

    @Override
    protected PipelineTransformationRequest getConsolidateRequest() {

        List<Action> actions = getValidTxnSoftDeleteActions(softDeleteActions);
        if (CollectionUtils.isEmpty(actions)) {
            log.info("No suitable delete actions for Transaction. Skip this step.");
            return null;
        } else {
            PipelineTransformationRequest request = new PipelineTransformationRequest();
            request.setName("SoftDeleteTransaction");

            List<TransformationStepConfig> steps = new ArrayList<>();

            List<Action> deleteActionsToMerge = processDeleteTimeSeriesActions(actions);

            int softDeleteMergeStep = 0;
            int softDeleteStep = 0; // might not have merge step

            // need to merge delete data
            if (CollectionUtils.isNotEmpty(deleteActionsToMerge)) {
                TransformationStepConfig softDeleteMerge = mergeTimeSeriesSoftDelete(actions,
                        InterfaceName.AccountId.name());
                steps.add(softDeleteMerge);
                softDeleteStep = softDeleteMergeStep + 1;
            } else {
                softDeleteMergeStep = -1;
            }

            int collectRawStep = softDeleteStep + 1;
            int cleanupRawStep = collectRawStep + 1;
            int dayPeriodStep = cleanupRawStep + 1;

            TransformationStepConfig softDelete = softDelete(softDeleteMergeStep,
                    InterfaceName.TransactionDayPeriod.name(), deleteTimeRanges);
            TransformationStepConfig collectRaw = collectRaw();
            TransformationStepConfig cleanupRaw = cleanupRaw(rawTable, collectRawStep);
            TransformationStepConfig dayPeriods = collectDays(softDeleteStep);
            TransformationStepConfig dailyPartition = partitionDaily(dayPeriodStep, softDeleteStep);

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

    // get all valid soft delete txn actions
    private List<Action> getValidTxnSoftDeleteActions(List<Action> softDeleteActions) {
        if (CollectionUtils.isEmpty(softDeleteActions)) {
            return Collections.emptyList();
        }

        return softDeleteActions.stream().filter(action -> {
            DeleteActionConfiguration config = (DeleteActionConfiguration) action.getActionConfiguration();
            boolean deleteByAccId = BusinessEntity.Account.equals(config.getIdEntity());
            boolean deleteByTimeRange = StringUtils.isNotBlank(config.getFromDate())
                    && StringUtils.isNotBlank(config.getToDate());
            return config.hasEntity(entity) && (deleteByAccId || deleteByTimeRange);
        }).collect(Collectors.toList());
    }

    private List<Action> processDeleteTimeSeriesActions(List<Action> actions) {
        List<Action> deleteDataToMerge = new ArrayList<>();
        for (Action action : actions) {
            DeleteActionConfiguration config = (DeleteActionConfiguration) action.getActionConfiguration();
            if (StringUtils.isNotBlank(config.getDeleteDataTable())) {
                deleteDataToMerge.add(action);
            } else {
                Preconditions.checkArgument(StringUtils.isNotBlank(config.getFromDate()),
                        "Delete action need to have either delete data or time range. Missing FromDate in time range");
                Preconditions.checkArgument(StringUtils.isNotBlank(config.getToDate()),
                        "Delete action need to have either delete data or time range. Missing ToDate in time range");
                deleteTimeRanges.add(parseTimeRange(config));
            }
        }
        return deleteDataToMerge;
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
