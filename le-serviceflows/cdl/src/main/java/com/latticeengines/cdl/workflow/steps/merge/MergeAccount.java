package com.latticeengines.cdl.workflow.steps.merge;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_COPIER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MATCH;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyUtils;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidateDataTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.CopierConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.MatchTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;

@Component(MergeAccount.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MergeAccount extends BaseSingleEntityMergeImports<ProcessAccountStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(MergeAccount.class);

    static final String BEAN_NAME = "mergeAccount";

    private int mergeStep;
    //private int slimInputStep;
    private int matchStep;
    private int fetchOnlyMatchStep;
    //private int slimDiffStep;
    //private int mergeMatchStep;
    private int upsertMasterStep;
    //private int slimMasterStep;
    private int diffStep;

    public PipelineTransformationRequest getConsolidateRequest() {
        try {

            PipelineTransformationRequest request = new PipelineTransformationRequest();
            request.setName("MergeAccount");

            mergeStep = 0;
            // slimInputStep = 1;
            matchStep = 1;
            if (configuration.isEntityMatchEnabled()) {
                fetchOnlyMatchStep = 2;
                // slimDiffStep = 3;
                upsertMasterStep = 3;
                // slimMasterStep = 6;
                diffStep = 4;
            } else {
                // slimDiffStep = 3;
                upsertMasterStep = 2;
                // slimMasterStep = 6;
                diffStep = 3;
            }

            TransformationStepConfig merge = mergeInputs(false, true, false);
            //TransformationStepConfig slimInputs = createSlimInputs(Collections.singletonList(mergeStep));
            TransformationStepConfig match = match(Collections.singletonList(mergeStep));

            TransformationStepConfig fetchOnlyMatch = null;
            //TransformationStepConfig slimDiff = null;
            //TransformationStepConfig mergeMatch = null;
            TransformationStepConfig upsertMaster;

            if (configuration.isEntityMatchEnabled()) {
                fetchOnlyMatch = fetchOnlyMatch(Collections.singletonList(matchStep));
                //slimDiff = createSlimTable(Collections.singletonList(fetchOnlyMatchStep), diffTablePrefix);
                //mergeMatch = mergeMatch(Arrays.asList(mergeStep, fetchOnlyMatchStep));
                upsertMaster = mergeMaster(Collections.singletonList(fetchOnlyMatchStep));
            } else {
                //slimDiff = createSlimTable(Collections.singletonList(matchStep), diffTablePrefix);
                //mergeMatch = mergeMatch(Arrays.asList(mergeStep, matchStep));
                upsertMaster = mergeMaster(Collections.singletonList(matchStep));
            }
            //TransformationStepConfig slimMaster = createSlimTable(
            //        Collections.singletonList(upsertMasterStep), batchStoreTablePrefix);
            TransformationStepConfig diff = diff(mergeStep, upsertMasterStep);
            TransformationStepConfig report = reportDiff(diffStep);

            List<TransformationStepConfig> steps = new ArrayList<>();
            steps.add(merge);
            //steps.add(slimInputs);
            steps.add(match);
            if (configuration.isEntityMatchEnabled()) {
                steps.add(fetchOnlyMatch);
            }
            //steps.add(slimDiff);
            //steps.add(mergeMatch);
            steps.add(upsertMaster);
            //steps.add(slimMaster);
            steps.add(diff);
            steps.add(report);
            request.setSteps(steps);
            return request;

        } catch (Exception e) {
            log.error("Failed to run consolidate data pipeline!", e);
            throw new RuntimeException(e);
        }
    }

    private TransformationStepConfig createSlimInputs(List<Integer> inputSteps) {
        TransformationStepConfig step = new TransformationStepConfig();
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

    private TransformationStepConfig match(List<Integer> inputSteps) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(inputSteps);
        step.setTransformer(TRANSFORMER_MATCH);
        step.setConfiguration(getMatchConfig());
        return step;
    }

    private TransformationStepConfig fetchOnlyMatch(List<Integer> inputSteps) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(inputSteps);
        step.setTransformer(TRANSFORMER_MATCH);
        step.setConfiguration(getFetchOnlyMatchConfig());
        return step;
    }

    private TransformationStepConfig mergeMaster(List<Integer> inputSteps) {
        TargetTable targetTable;
        TransformationStepConfig step = new TransformationStepConfig();
        setupMasterTable(step);
        step.setInputSteps(inputSteps);
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
        MatchInput matchInput = getBaseMatchInput();
        matchInput.setPredefinedSelection(ColumnSelection.Predefined.ID);

        if (configuration.isEntityMatchEnabled()) {
            matchInput.setOperationalMode(OperationalMode.ENTITY_MATCH);
            matchInput.setSkipKeyResolution(true);
            matchInput.setTargetEntity(BusinessEntity.Account.name());
            matchInput.setAllocateId(true);
            matchInput.setFetchOnly(false);

            EntityKeyMap entityKeyMap = new EntityKeyMap();
            entityKeyMap.setKeyMap(getMatchKeys());
            if (MapUtils.isNotEmpty(entityKeyMap.getKeyMap())) {
                if (entityKeyMap.getKeyMap().containsKey(MatchKey.SystemId)) {
                    // This should not happen because nothing is setting SystemId.
                    log.error("SystemId somehow set in KeyMap before MergeAccount!");
                } else {
                    // TODO(jwinter): Support other SystemIds in M28.
                    // For now, we hard code the SystemID MatchKey and SystemId Priority List to contain only AccountId.
                    List<String> systemIdList = Collections.singletonList(InterfaceName.AccountId.toString());
                    entityKeyMap.getKeyMap().put(MatchKey.SystemId, systemIdList);
                    entityKeyMap.setSystemIdPriority(systemIdList);
                }
            }

            Map<String, EntityKeyMap> entityKeyMaps = new HashMap<>();
            entityKeyMaps.put(BusinessEntity.Account.name(), entityKeyMap);
            matchInput.setEntityKeyMaps(entityKeyMaps);
        } else {
            // Non-Entity Match only configuration of MatchInput.
            matchInput.setOperationalMode(OperationalMode.LDC_MATCH);
            matchInput.setSkipKeyResolution(false);
            matchInput.setKeyMap(getMatchKeys());
            matchInput.setPartialMatchEnabled(true);
        }
        config.setMatchInput(matchInput);
        return JsonUtils.serialize(config);
    }

    private String getFetchOnlyMatchConfig() {
        MatchTransformerConfig config = new MatchTransformerConfig();
        MatchInput matchInput = getBaseMatchInput();
        matchInput.setOperationalMode(OperationalMode.ENTITY_MATCH);
        matchInput.setSkipKeyResolution(true);
        matchInput.setTargetEntity(BusinessEntity.Account.name());

        // Fetch Only Match specific settings.
        matchInput.setPredefinedSelection(ColumnSelection.Predefined.Seed);
        matchInput.setAllocateId(false);
        matchInput.setFetchOnly(true);

        // Prepare Entity Key Map for Fetch Oly Match.
        Map<MatchKey, List<String>> keyMap = //
                MatchKeyUtils.resolveKeyMap(Collections.singletonList(InterfaceName.EntityId.name()));
        keyMap.put(MatchKey.EntityId, Collections.singletonList(InterfaceName.EntityId.name()));
        EntityKeyMap entityKeyMap = new EntityKeyMap();
        entityKeyMap.setKeyMap(keyMap);
        entityKeyMap.setSystemIdPriority(Collections.emptyList());
        Map<String, EntityKeyMap> entityKeyMaps = new HashMap<>();
        entityKeyMaps.put(BusinessEntity.Account.name(), entityKeyMap);
        matchInput.setEntityKeyMaps(entityKeyMaps);

        config.setMatchInput(matchInput);
        return JsonUtils.serialize(config);
    }

    private Map<MatchKey, List<String>> getMatchKeys() {
        Set<String> names = getInputTableColumnNames(0);
        Map<MatchKey, List<String>> matchKeys = new HashMap<>();
        addLDCMatchKeysIfExist(names, matchKeys);
        log.info("Using match keys: " + JsonUtils.serialize(matchKeys));
        return matchKeys;
    }

    @Override
    protected void enrichTableSchema(Table table) {
        List<Attribute> attrs = new ArrayList<>();
        table.getAttributes().forEach(attr0 -> {
            attr0.setTags(Tag.INTERNAL);
            attrs.add(attr0);
        });
        table.setAttributes(attrs);
        metadataProxy.updateTable(customerSpace.toString(), table.getName(), table);
    }

    @Override
    protected void onPostTransformationCompleted() {
        super.onPostTransformationCompleted();
        // registerDiffSlim();
        // registerMasterSlim();

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
