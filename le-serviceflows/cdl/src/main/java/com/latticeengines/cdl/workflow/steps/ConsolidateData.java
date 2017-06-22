package com.latticeengines.cdl.workflow.steps;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_BUCKETER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MATCH;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_SORTER;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidateDataTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidateDeltaTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.MatchTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.SorterConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.ConsolidateDataConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformWrapperStep;

@Component("consolidateData")
public class ConsolidateData extends BaseTransformWrapperStep<ConsolidateDataConfiguration> {

    private static final Log log = LogFactory.getLog(ConsolidateData.class);

    private String masterTableName;
    private String profileTableName;
    private static final String consolidatedTableName = "ConsolidatedTable";
    private static final String accountId = TableRoleInCollection.ConsolidatedAccount.getPrimaryKey().name();
    private static final List<String> masterTableSortKeys = TableRoleInCollection.ConsolidatedAccount
            .getForeignKeysAsStringList();

    private CustomerSpace customerSpace = null;

    @Autowired
    protected MetadataProxy metadataProxy;

    @Autowired
    private DataCollectionProxy dataCollectionProxy;

    private List<String> inputTableNames = new ArrayList<>();
    private String srcIdField;
    Map<MatchKey, List<String>> keyMap = null;
    Boolean isActive = false;
    private String collectionName;

    private int mergeStep;
    private int matchStep;
    private int upsertMasterStep;
    private int consolidatedStep;
    private int bucketStep;
    private int sortStep;

    @Override
    protected TransformationWorkflowConfiguration executePreTransformation() {
        initializeConfiguration();
        return generateWorkflowConf();
    }

    @Override
    protected void onPostTransformationCompleted() {

        Table newMasterTable = metadataProxy.getTable(customerSpace.toString(),
                TableUtils.getFullTableName(masterTableName, pipelineVersion));
        dataCollectionProxy.upsertTable(customerSpace.toString(), collectionName, newMasterTable.getName(),
                TableRoleInCollection.ConsolidatedAccount);
        if (isBucketing()) {
            Table diffTable = metadataProxy.getTable(customerSpace.toString(),
                    TableUtils.getFullTableName(consolidatedTableName, pipelineVersion));
            putObjectInContext(TABLE_GOING_TO_REDSHIFT, diffTable);
        }
        putObjectInContext(CONSOLIDATE_MASTER_TABLE, newMasterTable);
        putObjectInContext(CONSOLIDATE_DOING_PUBLISH, isBucketing());
    }

    private void initializeConfiguration() {
        customerSpace = configuration.getCustomerSpace();
        List<Table> inputTables = getListObjectFromContext(CONSOLIDATE_INPUT_TABLES, Table.class);
        if (inputTables == null || inputTables.isEmpty()) {
            throw new RuntimeException("There is no input tables to consolidate.");
        }
        inputTables.sort(Comparator.comparing((Table t) -> t.getLastModifiedKey() == null ? -1
                : t.getLastModifiedKey().getLastModifiedTimestamp() == null ? -1
                        : t.getLastModifiedKey().getLastModifiedTimestamp())
                .reversed());
        for (Table table : inputTables) {
            inputTableNames.add(table.getName());
        }

        collectionName = configuration.getDataCollectionName();
        Table masterTable = dataCollectionProxy.getTable(customerSpace.toString(), collectionName,
                TableRoleInCollection.ConsolidatedAccount);
        if (masterTable == null || masterTable.getExtracts().isEmpty()) {
            log.info("There has been no master table for this data collection. Creating a new one");
            masterTableName = NamingUtils.timestamp("Account");
        } else {
            masterTableName = masterTable.getName();
        }
        log.info("Set masterTableName=" + masterTableName);

        srcIdField = configuration.getIdField();
        keyMap = configuration.getMatchKeyMap();

        isActive = getObjectFromContext(IS_ACTIVE, Boolean.class);
        if (isBucketing()) {
            Table profileTable = dataCollectionProxy.getTable(customerSpace.toString(), collectionName,
                    TableRoleInCollection.Profile);
            profileTableName = profileTable.getName();
            log.info("Set profileTableName=" + profileTableName);
        }
    }

    private TransformationWorkflowConfiguration generateWorkflowConf() {
        PipelineTransformationRequest request = getConcolidateReqest();
        return transformationProxy.getWorkflowConf(request, configuration.getPodId());
    }

    private boolean isBucketing() {
        return !Boolean.TRUE.equals(isActive);
    }

    private PipelineTransformationRequest getConcolidateReqest() {
        try {

            PipelineTransformationRequest request = new PipelineTransformationRequest();
            request.setName("ConsolidatePipeline");

            mergeStep = 0;
            matchStep = 1;
            upsertMasterStep = 2;
            consolidatedStep = 3;
            bucketStep = 4;
            sortStep = 5;

            TransformationStepConfig merge = createMergeStep();
            TransformationStepConfig match = createMatchStep();
            TransformationStepConfig upsertMaster = createUpsertMasterStep();
            TransformationStepConfig consolidate = createConsolidatedStep();
            TransformationStepConfig bucket = createBucketStep();
            TransformationStepConfig sort = createSorterStep();

            List<TransformationStepConfig> steps = new ArrayList<>();
            steps.add(merge);
            steps.add(match);
            steps.add(upsertMaster);
            steps.add(consolidate);
            if (isBucketing()) {
                steps.add(bucket);
                steps.add(sort);
            }
            request.setSteps(steps);
            if (isBucketing()) {
                request.setContainerMemMB(workflowMemMb);
            } else {
                request.setContainerMemMB(workflowMemMbMax);
            }
            return request;

        } catch (Exception e) {
            log.error("Failed to run consolidate data pipeline!", e);
            throw new RuntimeException(e);
        }
    }

    private TransformationStepConfig createMergeStep() {
        TransformationStepConfig step1 = new TransformationStepConfig();
        List<String> baseSources = inputTableNames;
        step1.setBaseSources(baseSources);

        Map<String, SourceTable> baseTables = new HashMap<>();
        for (String inputTableName : inputTableNames) {
            baseTables.put(inputTableName, new SourceTable(inputTableName, customerSpace));
        }
        step1.setBaseTables(baseTables);
        step1.setTransformer("consolidateDataTransformer");
        step1.setConfiguration(getConsolidateDataConfig());
        return step1;
    }

    private TransformationStepConfig createMatchStep() {
        TransformationStepConfig step2 = new TransformationStepConfig();
        // step 1 output
        step2.setInputSteps(Collections.singletonList(mergeStep));
        step2.setTransformer(TRANSFORMER_MATCH);
        step2.setConfiguration(getMatchConfig());
        return step2;
    }

    private TransformationStepConfig createUpsertMasterStep() {
        List<String> baseSources;
        Map<String, SourceTable> baseTables;
        TargetTable targetTable;
        TransformationStepConfig step3 = new TransformationStepConfig();
        Table masterTable = metadataProxy.getTable(customerSpace.toString(), masterTableName);
        if (masterTable != null && !masterTable.getExtracts().isEmpty()) {
            baseSources = Collections.singletonList(masterTableName);
            baseTables = new HashMap<>();
            SourceTable sourceMasterTable = new SourceTable(masterTableName, customerSpace);
            baseTables.put(masterTableName, sourceMasterTable);
            step3.setBaseSources(baseSources);
            step3.setBaseTables(baseTables);
        }
        step3.setInputSteps(Collections.singletonList(matchStep));
        step3.setTransformer("consolidateDataTransformer");
        step3.setConfiguration(getConsolidateDataConfig());

        targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(masterTableName);
        targetTable.setPrimaryKey(accountId);
        step3.setTargetTable(targetTable);
        return step3;
    }

    private TransformationStepConfig createConsolidatedStep() {
        TransformationStepConfig step4 = new TransformationStepConfig();
        step4.setInputSteps(Arrays.asList(matchStep, upsertMasterStep));
        step4.setTransformer("consolidateDeltaTransformer");
        step4.setConfiguration(getConsolidateDeltaConfig());
        if (!isBucketing()) {
            TargetTable targetTable = new TargetTable();
            targetTable.setCustomerSpace(customerSpace);
            targetTable.setNamePrefix(consolidatedTableName);
            targetTable.setPrimaryKey(accountId);
            step4.setTargetTable(targetTable);
        }
        return step4;
    }

    private TransformationStepConfig createBucketStep() {
        if (!isBucketing()) {
            return null;
        }
        TransformationStepConfig step5 = new TransformationStepConfig();
        step5.setBaseSources(Collections.singletonList(profileTableName));
        // consolidate diff
        step5.setInputSteps(Collections.singletonList(consolidatedStep));
        step5.setTransformer(TRANSFORMER_BUCKETER);
        step5.setConfiguration(emptyStepConfig(lightEngineConfig()));
        return step5;
    }

    private TransformationStepConfig createSorterStep() {
        if (!isBucketing()) {
            return null;
        }
        TransformationStepConfig step6 = new TransformationStepConfig();
        step6.setInputSteps(Collections.singletonList(bucketStep));
        step6.setTransformer(TRANSFORMER_SORTER);
        step6.setConfiguration(sortStepConfiguration());

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(consolidatedTableName);
        targetTable.setPrimaryKey(srcIdField);
        step6.setTargetTable(targetTable);
        return step6;
    }

    private String sortStepConfiguration() {
        try {
            SorterConfig config = new SorterConfig();
            config.setPartitions(100);
            config.setSortingField(masterTableSortKeys.get(0)); // TODO: only support single sorting key now
            config.setCompressResult(false);
            return appendEngineConf(config, lightEngineConfig());
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private String getConsolidateDataConfig() {
        ConsolidateDataTransformerConfig config = new ConsolidateDataTransformerConfig();
        config.setSrcIdField(srcIdField);
        return appendEngineConf(config, lightEngineConfig());
    }

    private String getConsolidateDeltaConfig() {
        ConsolidateDeltaTransformerConfig config = new ConsolidateDeltaTransformerConfig();
        config.setSrcIdField(srcIdField);
        return appendEngineConf(config, lightEngineConfig());
    }

    private String getMatchConfig() {
        MatchTransformerConfig config = new MatchTransformerConfig();
        MatchInput matchInput = new MatchInput();
        matchInput.setRootOperationUid(UUID.randomUUID().toString().toUpperCase());
        matchInput.setTenant(new Tenant(customerSpace.getTenantId()));
        matchInput.setPredefinedSelection(Predefined.ID);
        if (keyMap == null) {
            matchInput.setKeyMap(getKeyMap());
        } else {
            matchInput.setKeyMap(keyMap);
        }
        matchInput.setExcludeUnmatchedWithPublicDomain(false);
        matchInput.setPublicDomainAsNormalDomain(true);
        matchInput.setDataCloudVersion(getDataCloudVersion());
        matchInput.setSkipKeyResolution(false);
        matchInput.setUseDnBCache(true);
        matchInput.setUseRemoteDnB(true);
        matchInput.setLogDnBBulkResult(false);
        matchInput.setMatchDebugEnabled(false);
        config.setMatchInput(matchInput);
        return JsonUtils.serialize(config);
    }

    private Map<MatchKey, List<String>> getKeyMap() {
        Map<MatchKey, List<String>> keyMap = new HashMap<>();
        keyMap.put(MatchKey.LatticeAccountID, Collections.singletonList(InterfaceName.LatticeAccountId.name()));
        return keyMap;
    }

}
