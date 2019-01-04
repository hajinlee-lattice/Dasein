package com.latticeengines.cdl.workflow.steps.merge;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_COPIER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MATCH;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput.EntityKeyMap;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.BulkMatchMergerTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidateDataTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.CopierConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.MatchTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;

@Component(MergeAccount.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MergeAccount extends BaseSingleEntityMergeImports<ProcessAccountStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(MergeAccount.class);

    static final String BEAN_NAME = "mergeAccount";

    private int mergeStep;
    private int slimInputStep;
    private int matchStep;
    private int slimDiffStep;
    private int mergeMatchStep;
    private int upsertMasterStep;
    private int slimMasterStep;
    private int diffStep;

    public PipelineTransformationRequest getConsolidateRequest() {
        try {

            PipelineTransformationRequest request = new PipelineTransformationRequest();
            request.setName("MergeAccount");

            mergeStep = 0;
            slimInputStep = 1;
            matchStep = 2;
            slimDiffStep = 3;
            mergeMatchStep = 4;
            upsertMasterStep = 5;
            slimMasterStep = 6;
            diffStep = 7;

            TransformationStepConfig merge = mergeInputs(false, true, false);
            TransformationStepConfig slimInputs = createSlimInputs();
            TransformationStepConfig match = match();
            TransformationStepConfig slimDiff = createSlimTable(matchStep, diffTablePrefix);
            TransformationStepConfig mergeMatch = mergeMatch(mergeStep, matchStep);
            TransformationStepConfig upsertMaster = mergeMaster();
            TransformationStepConfig slimMaster = createSlimTable(upsertMasterStep, batchStoreTablePrefix);
            TransformationStepConfig diff = diff(mergeStep, upsertMasterStep);
            TransformationStepConfig report = reportDiff(diffStep);

            List<TransformationStepConfig> steps = new ArrayList<>();
            steps.add(merge);
            steps.add(slimInputs);
            steps.add(match);
            steps.add(slimDiff);
            steps.add(mergeMatch);
            steps.add(upsertMaster);
            steps.add(slimMaster);
            steps.add(diff);
            steps.add(report);
            request.setSteps(steps);
            return request;

        } catch (Exception e) {
            log.error("Failed to run consolidate data pipeline!", e);
            throw new RuntimeException(e);
        }
    }

    private TransformationStepConfig createSlimInputs() {
        TransformationStepConfig step = new TransformationStepConfig();
        List<Integer> inputSteps = Collections.singletonList(mergeStep);
        step.setInputSteps(inputSteps);
        step.setTransformer(TRANSFORMER_COPIER);

        CopierConfig conf = new CopierConfig();
        Map<MatchKey, List<String>> matchKeys = getMatchKeys();
        List<String> retainFields = getRetainFields(matchKeys);
        conf.setRetainAttrs(retainFields);
        String confStr = appendEngineConf(conf, heavyEngineConfig());
        step.setConfiguration(confStr);
        return step;
    }

    private List<String> getRetainFields(Map<MatchKey, List<String>> matchKeys) {
        List<String> fields = new ArrayList<>();
        fields.add(batchStorePrimaryKey);
        for (List<String> values : matchKeys.values()) {
            if (CollectionUtils.isNotEmpty(values)) {
                fields.addAll(values);
            }
        }
        return fields;
    }

    private TransformationStepConfig createSlimTable(int inputStep, String tablePrefix) {
        TransformationStepConfig step = new TransformationStepConfig();
        List<Integer> steps = Collections.singletonList(inputStep);
        step.setInputSteps(steps);
        step.setTransformer(TRANSFORMER_COPIER);

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(tablePrefix + "_Slim");
        step.setTargetTable(targetTable);

        CopierConfig conf = new CopierConfig();
        List<String> slimFields = Arrays.asList(batchStorePrimaryKey, InterfaceName.LatticeAccountId.name());
        conf.setRetainAttrs(slimFields);
        String confStr = appendEngineConf(conf, heavyEngineConfig());
        step.setConfiguration(confStr);
        return step;
    }

    private TransformationStepConfig match() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(slimInputStep));
        step.setTransformer(TRANSFORMER_MATCH);
        step.setConfiguration(getMatchConfig());
        return step;
    }

    private TransformationStepConfig mergeMatch(int mergeStep, int matchStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        List<Integer> steps = Arrays.asList(mergeStep, matchStep, matchStep);
        step.setInputSteps(steps);
        step.setTransformer("bulkMatchMergerTransformer");

        BulkMatchMergerTransformerConfig conf = new BulkMatchMergerTransformerConfig();
        conf.setJoinField(batchStorePrimaryKey);
        String confStr = appendEngineConf(conf, heavyEngineConfig());
        step.setConfiguration(confStr);
        return step;
    }

    private TransformationStepConfig mergeMaster() {
        TargetTable targetTable;
        TransformationStepConfig step = new TransformationStepConfig();
        setupMasterTable(step);
        step.setInputSteps(Collections.singletonList(mergeMatchStep));
        step.setTransformer(DataCloudConstants.TRANSFORMER_CONSOLIDATE_DATA);
        step.setConfiguration(getMergeMasterConfig());

        targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(batchStoreTablePrefix);
        step.setTargetTable(targetTable);
        return step;
    }

    private String getMergeMasterConfig() {
        ConsolidateDataTransformerConfig config = new ConsolidateDataTransformerConfig();
        config.setSrcIdField(InterfaceName.Id.name());
        config.setMasterIdField(TableRoleInCollection.ConsolidatedAccount.getPrimaryKey().name());
        config.setColumnsFromRight(Collections.singleton(InterfaceName.CDLCreatedTime.name()));
        return appendEngineConf(config, heavyEngineConfig());
    }

    private String getMatchConfig() {
        MatchTransformerConfig config = new MatchTransformerConfig();
        MatchInput matchInput = new MatchInput();
        matchInput.setRootOperationUid(UUID.randomUUID().toString().toUpperCase());
        matchInput.setTenant(new Tenant(customerSpace.getTenantId()));
        matchInput.setPredefinedSelection(ColumnSelection.Predefined.ID);
        matchInput.setExcludePublicDomain(false);
        matchInput.setPublicDomainAsNormalDomain(false);
        matchInput.setDataCloudVersion(getDataCloudVersion());
        // TODO(lming): This won't work for Entity Match case.  Need to set entityKeyMapList instead.
        matchInput.setKeyMap(getMatchKeys());
        matchInput.setSkipKeyResolution(false);
        matchInput.setUseDnBCache(true);
        matchInput.setUseRemoteDnB(true);
        matchInput.setLogDnBBulkResult(false);
        matchInput.setMatchDebugEnabled(false);
        matchInput.setPartialMatchEnabled(true);
        matchInput.setSplitsPerBlock(cascadingPartitions * 10);
        // TODO(lming): We should set the correct operational mode for other match cases here.
        if (configuration.isEntityMatchEnabled()) {
            matchInput.setOperationalMode(OperationalMode.ENTITY_MATCH);
            // TODO(lming): Allocate ID should be set appropriately.
            matchInput.setAllocateId(true);
            // TODO(lming): Is it safe to assume here that the target entity is always Account?
            matchInput.setTargetEntity(BusinessEntity.Account.name());

            EntityKeyMap entityKeyMap = new EntityKeyMap();
            entityKeyMap.setBusinessEntity(BusinessEntity.Account.name());
            entityKeyMap.setKeyMap(getMatchKeys());
            // TODO(lming): Not sure how the System ID priority is supposed to get set up.  For now, copy from the
            //     Key Map.
            if (MapUtils.isNotEmpty(entityKeyMap.getKeyMap())
                    && entityKeyMap.getKeyMap().containsKey(MatchKey.SystemId)) {
                entityKeyMap.setSystemIdPriority(entityKeyMap.getKeyMap().get(MatchKey.SystemId));
            }
            List<EntityKeyMap> entityKeyMapList = new ArrayList<>();
            entityKeyMapList.add(entityKeyMap);
            matchInput.setEntityKeyMapList(entityKeyMapList);
        }
        // TODO(lming): Need to set targetEntity and allocateId.
        config.setMatchInput(matchInput);
        return JsonUtils.serialize(config);
    }

    private Map<MatchKey, List<String>> getMatchKeys() {
        String tableName = inputTableNames.get(0);
        List<ColumnMetadata> cms = metadataProxy.getTableColumns(customerSpace.toString(), tableName);
        Set<String> names = cms.stream().map(ColumnMetadata::getAttrName).collect(Collectors.toSet());
        Map<MatchKey, List<String>> matchKeys = new HashMap<>();
        addMatchKeyIfExists(names, matchKeys, MatchKey.Domain, InterfaceName.Website.name());
        addMatchKeyIfExists(names, matchKeys, MatchKey.DUNS, InterfaceName.DUNS.name());

        addMatchKeyIfExists(names, matchKeys, MatchKey.Name, InterfaceName.CompanyName.name());
        addMatchKeyIfExists(names, matchKeys, MatchKey.City, InterfaceName.City.name());
        addMatchKeyIfExists(names, matchKeys, MatchKey.State, InterfaceName.State.name());
        addMatchKeyIfExists(names, matchKeys, MatchKey.Country, InterfaceName.Country.name());

        addMatchKeyIfExists(names, matchKeys, MatchKey.PhoneNumber, InterfaceName.PhoneNumber.name());
        addMatchKeyIfExists(names, matchKeys, MatchKey.Zipcode, InterfaceName.PostalCode.name());

        log.info("Using match keys: " + JsonUtils.serialize(matchKeys));
        return matchKeys;
    }

    private void addMatchKeyIfExists(Set<String> cols, Map<MatchKey, List<String>> keyMap, MatchKey key,
            String columnName) {
        if (cols.contains(columnName)) {
            keyMap.put(key, Collections.singletonList(columnName));
        }
    }

    @Override
    protected Table enrichTableSchema(Table table) {
        List<Attribute> attrs = new ArrayList<>();
        table.getAttributes().forEach(attr0 -> {
            attr0.setTags(Tag.INTERNAL);
            attrs.add(attr0);
        });
        table.setAttributes(attrs);
        metadataProxy.updateTable(customerSpace.toString(), table.getName(), table);
        return table;
    }

    @Override
    protected void onPostTransformationCompleted() {
        super.onPostTransformationCompleted();
        registerDiffSlim();
        registerMasterSlim();

    }

    private void registerMasterSlim() {
        Table table = metadataProxy.getTable(customerSpace.toString(),
                TableUtils.getFullTableName(batchStoreTablePrefix + "_Slim", pipelineVersion));
        if (table == null) {
            throw new IllegalStateException("Did not generate new table as master Slim.");
        }
        dataCollectionProxy.upsertTable(customerSpace.toString(), table.getName(),
                TableRoleInCollection.AccountBatchSlim, inactive);
    }

    private void registerDiffSlim() {
        Table table = metadataProxy.getTable(customerSpace.toString(),
                TableUtils.getFullTableName(diffTablePrefix + "_Slim", pipelineVersion));
        if (table == null) {
            throw new IllegalStateException("Did not generate new table as diff Slim.");
        }
        dataCollectionProxy.upsertTable(customerSpace.toString(), table.getName(),
                TableRoleInCollection.AccountDiffSlim, inactive);

    }
}
