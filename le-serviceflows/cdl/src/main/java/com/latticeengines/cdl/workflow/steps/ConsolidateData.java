package com.latticeengines.cdl.workflow.steps;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidateDataTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidateDeltaTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.MatchTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

@Component("consolidateData")
public class ConsolidateData extends BaseTransformationStep<ConsolidateDataConfiguration> {

    private static final Log log = LogFactory.getLog(ConsolidateData.class);

    public static final String DATE_FORMAT_STRING = "yyyy-MM-dd_HH-mm-ss_z";
    public static final SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT_STRING);

    private static String masterTableName;
    private static final String mergedTableName = "MergedTable";
    private static final String consolidatedTableName = "ConsolidatedTable";

    private static final CustomerSpace customerSpace = CustomerSpace.parse(DataCloudConstants.SERVICE_CUSTOMERSPACE);

    @Autowired
    private ColumnMetadataProxy columnMetadataProxy;

    @Autowired
    protected MetadataProxy metadataProxy;

    private List<String> inputTableNames = new ArrayList<>();
    private String targetVersion;
    private String idField;
    Map<MatchKey, List<String>> keyMap = null;

    @Override
    public void onConfigurationInitialized() {
        List<Table> inputTables = getListObjectFromContext(CONSOLIDATE_INPUT_TABLES, Table.class);
        for (Table table : inputTables) {
            inputTableNames.add(table.getName());
        }

        masterTableName = configuration.getMasterTableName();
        idField = configuration.getIdField();
        keyMap = configuration.getMatchKeyMap();

        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        targetVersion = dateFormat.format(new Date());
    }

    @Override
    public void execute() {

        PipelineTransformationRequest request = getConcolidateReqest();

        TransformationProgress progress = transformationProxy.transform(request, null);
        waitForFinish(progress);

    }

    @Override
    public void onExecutionCompleted() {
        metadataProxy
                .deleteTable(customerSpace.toString(), TableUtils.getFullTableName(mergedTableName, targetVersion));

        Table consolidatedTable = metadataProxy.getTable(customerSpace.toString(),
                TableUtils.getFullTableName(consolidatedTableName, targetVersion));
        Table newMasterTable = metadataProxy.getTable(customerSpace.toString(),
                TableUtils.getFullTableName(masterTableName, targetVersion));
        newMasterTable.setName(masterTableName);
        metadataProxy.updateTable(customerSpace.toString(), masterTableName, newMasterTable);

        putObjectInContext(CONSOLIDATE_CONSOLIDATED_TABLE, consolidatedTable);
        putObjectInContext(CONSOLIDATE_MASTER_TABLE, newMasterTable);
    }

    private PipelineTransformationRequest getConcolidateReqest() {
        try {

            PipelineTransformationRequest request = new PipelineTransformationRequest();
            request.setName("ConsolidatePipeline");
            request.setVersion(targetVersion);

            TransformationStepConfig mergeStep = createMergeStep();
            TransformationStepConfig matchStep = createMatchStep();
            TransformationStepConfig upsertMasterStep = createUpsertMasterStep();
            TransformationStepConfig getConsolidatedStep = createConsolidatedStep();

            List<TransformationStepConfig> steps = Arrays.asList(mergeStep, matchStep, upsertMasterStep, getConsolidatedStep);
            request.setSteps(steps);

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

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(mergedTableName);
        step1.setTargetTable(targetTable);
        step1.setConfiguration(getConsolidateDataConfig());
        return step1;
    }

    private TransformationStepConfig createMatchStep() {
        TransformationStepConfig step2 = new TransformationStepConfig();
        // step 1 output
        step2.setInputSteps(Collections.singletonList(0));
        step2.setTransformer("bulkMatchTransformer");
        step2.setConfiguration(getMatchConfig());
        return step2;
    }

    private TransformationStepConfig createUpsertMasterStep() {
        List<String> baseSources;
        Map<String, SourceTable> baseTables;
        TargetTable targetTable;
        TransformationStepConfig step3 = new TransformationStepConfig();
        Table masterTable = metadataProxy.getTable(customerSpace.toString(), masterTableName);
        if (masterTable != null) {
            baseSources = Arrays.asList(masterTableName);
            baseTables = new HashMap<>();
            SourceTable sourceMasterTable = new SourceTable(masterTableName, customerSpace);
            baseTables.put(masterTableName, sourceMasterTable);
            step3.setBaseSources(baseSources);
            step3.setBaseTables(baseTables);
        }
        // step 2 output
        step3.setInputSteps(Collections.singletonList(1));
        step3.setTransformer("consolidateDataTransformer");
        step3.setConfiguration(getConsolidateDataConfig());

        targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(masterTableName);
        step3.setTargetTable(targetTable);
        return step3;
    }

    private TransformationStepConfig createConsolidatedStep() {
        TargetTable targetTable;
        TransformationStepConfig step4 = new TransformationStepConfig();
        // step 2, 3 output
        step4.setInputSteps(Arrays.asList(1, 2));
        step4.setTransformer("consolidateDeltaTransformer");
        step4.setConfiguration(getConsolidateDeltaConfig());

        targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(consolidatedTableName);
        step4.setTargetTable(targetTable);
        return step4;
    }

    private String getConsolidateDataConfig() {
        ConsolidateDataTransformerConfig config = new ConsolidateDataTransformerConfig();
        config.setSrcIdField(idField);
        return JsonUtils.serialize(config);
    }

    private String getConsolidateDeltaConfig() {
        ConsolidateDeltaTransformerConfig config = new ConsolidateDeltaTransformerConfig();
        config.setSrcIdField(idField);
        return JsonUtils.serialize(config);
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
        matchInput.setDecisionGraph("DragonClaw");
        matchInput.setExcludeUnmatchedWithPublicDomain(false);
        matchInput.setPublicDomainAsNormalDomain(true);
        matchInput.setDataCloudVersion(getDataCloudVersion());
        matchInput.setSkipKeyResolution(true);
        matchInput.setUseDnBCache(true);
        matchInput.setUseRemoteDnB(true);
        matchInput.setLogDnBBulkResult(false);
        matchInput.setMatchDebugEnabled(false);

        config.setMatchInput(matchInput);
        return JsonUtils.serialize(config);
    }

    private Map<MatchKey, List<String>> getKeyMap() {
        Map<MatchKey, List<String>> keyMap = new TreeMap<>();
        keyMap.put(MatchKey.Domain, Arrays.asList("Domain"));
        // keyMap.put(MatchKey.Name, Arrays.asList("Display Name"));
        // keyMap.put(MatchKey.Country, Arrays.asList("Country"));
        // keyMap.put(MatchKey.State, Arrays.asList("State"));
        // keyMap.put(MatchKey.City, Arrays.asList("City"));
        // keyMap.put(MatchKey.Zipcode, Arrays.asList("Zip"));
        // keyMap.put(MatchKey.PhoneNumber, Arrays.asList("PhoneNumber"));
        return keyMap;
    }

    private String getDataCloudVersion() {
        return columnMetadataProxy.latestVersion(null).getVersion();
    }

}
