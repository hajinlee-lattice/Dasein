package com.latticeengines.cdl.workflow.steps;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_BUCKETER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MATCH;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.UnionSelection;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidateDataTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.MatchTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.ConsolidateAccountDataStepConfiguration;

@Component("consolidateAccountData")
public class ConsolidateAccountData extends ConsolidateDataBase<ConsolidateAccountDataStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ConsolidateAccountData.class);

    private Map<MatchKey, List<String>> keyMap = null;

    private int mergeStep;
    private int mergeNewStep;
    private int matchStep;
    private int upsertMasterStep;
    private int diffStep;
    private int matchDiffStep;
    private int bucketStep;
    @SuppressWarnings("unused")
    private int sortStep;

    @Override
    protected void initializeConfiguration() {
        super.initializeConfiguration();
        srcIdField = configuration.getIdField();
        keyMap = configuration.getMatchKeyMap();
    }

    public PipelineTransformationRequest getConsolidateRequest() {
        try {

            PipelineTransformationRequest request = new PipelineTransformationRequest();
            request.setName("ConsolidatePipeline");

            mergeStep = 0;
            mergeNewStep = 1;
            matchStep = 2;
            upsertMasterStep = 3;
            diffStep = 4;
            matchDiffStep = 5;
            bucketStep = 6;
            sortStep = 7;

            TransformationStepConfig merge = mergeInputs();
            TransformationStepConfig mergeNew = mergeNew();
            TransformationStepConfig match = match();
            TransformationStepConfig upsertMaster = mergeMaster();
            TransformationStepConfig diff = diff(mergeStep, upsertMasterStep);
            TransformationStepConfig matchDiff = matchDiff();
            TransformationStepConfig bucket = bucketDiff();
            TransformationStepConfig sort = sortDiff(bucketStep);

            List<TransformationStepConfig> steps = new ArrayList<>();
            steps.add(merge);
            steps.add(mergeNew);
            steps.add(match);
            steps.add(upsertMaster);
            if (isBucketing()) {
                steps.add(diff);
                steps.add(matchDiff);
                steps.add(bucket);
                steps.add(sort);
            }
            request.setSteps(steps);
            return request;

        } catch (Exception e) {
            log.error("Failed to run consolidate data pipeline!", e);
            throw new RuntimeException(e);
        }
    }


    @Override
    protected void setupConfig(ConsolidateDataTransformerConfig config) {
        if (inputMasterTableName != null) {
            List<String> fields = AvroUtils.getSchemaFields(yarnConfiguration,
                    masterTable.getExtracts().get(0).getPath());
            config.setOrigMasterFields(fields);
        }
        config.setMasterIdField(TableRoleInCollection.ConsolidatedAccount.getPrimaryKey().name());

    }

    private TransformationStepConfig mergeNew() {
        TransformationStepConfig step2 = new TransformationStepConfig();
        setupMasterTable(step2);
        step2.setInputSteps(Collections.singletonList(mergeStep));
        step2.setTransformer("consolidateDeltaNewTransformer");
        step2.setConfiguration(getConsolidateDataConfig(false));
        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(newRecordsTablePrefix);
        targetTable.setPrimaryKey(batchStorePrimaryKey);
        step2.setTargetTable(targetTable);
        return step2;

    }

    private TransformationStepConfig match() {
        TransformationStepConfig step3 = new TransformationStepConfig();
        step3.setInputSteps(Collections.singletonList(mergeNewStep));
        step3.setTransformer(TRANSFORMER_MATCH);
        step3.setConfiguration(getMatchConfig());
        return step3;
    }

    private TransformationStepConfig mergeMaster() {
        TargetTable targetTable;
        TransformationStepConfig step4 = new TransformationStepConfig();
        setupMasterTable(step4);
        step4.setInputSteps(Arrays.asList(mergeStep, matchStep));
        step4.setTransformer("consolidateDataTransformer");
        step4.setConfiguration(getConsolidateDataMasterConfig());

        targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(batchStoreTablePrefix);
        targetTable.setPrimaryKey(batchStorePrimaryKey);
        step4.setTargetTable(targetTable);
        return step4;
    }

    private TransformationStepConfig matchDiff() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(TRANSFORMER_MATCH);
        step.setInputSteps(Collections.singletonList(diffStep));

        // Match Input
        MatchTransformerConfig config = new MatchTransformerConfig();
        MatchInput matchInput = new MatchInput();
        matchInput.setTenant(new Tenant(customerSpace.toString()));
        UnionSelection us = new UnionSelection();
        Map<Predefined, String> ps = new HashMap<>();
        ps.put(Predefined.Segment, "2.0");
        ColumnSelection cs = new ColumnSelection();
        List<Column> cols = Arrays.asList(new Column(DataCloudConstants.ATTR_LDC_DOMAIN),
                new Column(DataCloudConstants.ATTR_LDC_NAME));
        cs.setColumns(cols);
        us.setPredefinedSelections(ps);
        us.setCustomSelection(cs);
        matchInput.setUnionSelection(us);

        Map<MatchKey, List<String>> keyMap = new TreeMap<>();
        keyMap.put(MatchKey.LatticeAccountID, Collections.singletonList(InterfaceName.LatticeAccountId.name()));
        matchInput.setKeyMap(keyMap);

        matchInput.setDataCloudVersion(getDataCloudVersion());
        matchInput.setSkipKeyResolution(true);
        matchInput.setFetchOnly(true);
        matchInput.setSplitsPerBlock(cascadingPartitions * 10);
        config.setMatchInput(matchInput);

        step.setConfiguration(JsonUtils.serialize(config));
        return step;
    }

    private TransformationStepConfig bucketDiff() {
        if (!isBucketing()) {
            return null;
        }
        TransformationStepConfig step = new TransformationStepConfig();
        String tableSourceName = "CustomerProfile";
        SourceTable sourceTable = new SourceTable(profileTableName, customerSpace);
        List<String> baseSources = Collections.singletonList(tableSourceName);
        step.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableSourceName, sourceTable);
        step.setBaseTables(baseTables);
        // consolidate diff
        step.setInputSteps(Collections.singletonList(matchDiffStep));
        step.setTransformer(TRANSFORMER_BUCKETER);
        step.setConfiguration(emptyStepConfig(heavyEngineConfig()));
        return step;
    }

    private String getConsolidateDataMasterConfig() {
        ConsolidateDataTransformerConfig config = new ConsolidateDataTransformerConfig();
        config.setSrcIdField(srcIdField);
        config.setMasterIdField(TableRoleInCollection.ConsolidatedAccount.getPrimaryKey().name());
        config.setCreateTimestampColumn(true);
        config.setColumnsFromRight(new HashSet<String>(Arrays.asList(CREATION_DATE)));
        return appendEngineConf(config, lightEngineConfig());
    }

    private String getMatchConfig() {
        MatchTransformerConfig config = new MatchTransformerConfig();
        MatchInput matchInput = new MatchInput();
        matchInput.setRootOperationUid(UUID.randomUUID().toString().toUpperCase());
        matchInput.setTenant(new Tenant(customerSpace.getTenantId()));
        matchInput.setPredefinedSelection(Predefined.ID);
        if (keyMap != null) {
            matchInput.setKeyMap(keyMap);
        }
        matchInput.setExcludePublicDomain(false);
        matchInput.setPublicDomainAsNormalDomain(true);
        matchInput.setDataCloudVersion(getDataCloudVersion());
        matchInput.setSkipKeyResolution(false);
        matchInput.setUseDnBCache(true);
        matchInput.setUseRemoteDnB(true);
        matchInput.setLogDnBBulkResult(false);
        matchInput.setMatchDebugEnabled(false);
        matchInput.setPartialMatchEnabled(true);
        matchInput.setSplitsPerBlock(cascadingPartitions * 10);
        config.setMatchInput(matchInput);
        return JsonUtils.serialize(config);
    }

}
