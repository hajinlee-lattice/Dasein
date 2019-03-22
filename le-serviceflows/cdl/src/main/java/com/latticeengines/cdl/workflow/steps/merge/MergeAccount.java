package com.latticeengines.cdl.workflow.steps.merge;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MATCH;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
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
    private int matchStep;
    private int fetchOnlyMatchStep;
    private int upsertMasterStep;
    private int diffStep;

    private String diffTableNameInContext;
    private String batchStoreNameInContext;

    private boolean shortCutMode;

    @Override
    public PipelineTransformationRequest getConsolidateRequest() {
        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName("MergeAccount");

        diffTableNameInContext = getStringValueFromContext(ACCOUNT_DIFF_TABLE_NAME);
        batchStoreNameInContext = getStringValueFromContext(ACCOUNT_MASTER_TABLE_NAME);
        Table diffTableInContext = StringUtils.isNotBlank(diffTableNameInContext) ? //
                metadataProxy.getTable(customerSpace.toString(), diffTableNameInContext) : null;
        Table batchStoreInContext = StringUtils.isNotBlank(batchStoreNameInContext) ? //
                metadataProxy.getTable(customerSpace.toString(), batchStoreNameInContext) : null;
        if (diffTableInContext != null && batchStoreInContext != null) {
            log.info("Found diff table and batch store in context, using short-cut pipeline");
            shortCutMode = true;
            diffTableName = diffTableNameInContext;
            request.setSteps(shortCutSteps());
        } else {
            log.info("diffTableInContext is null: " + (diffTableInContext == null));
            log.info("batchStoreInContext is null: " + (batchStoreInContext == null));
            request.setSteps(regularSteps());
        }

        return request;
    }

    private List<TransformationStepConfig> regularSteps() {
        mergeStep = 0;
        matchStep = 1;
        if (configuration.isEntityMatchEnabled()) {
            fetchOnlyMatchStep = 2;
            upsertMasterStep = 3;
            diffStep = 4;
        } else {
            upsertMasterStep = 2;
            diffStep = 3;
        }

        TransformationStepConfig merge = mergeInputs(false, true, false);
        TransformationStepConfig match = match(Collections.singletonList(mergeStep));

        TransformationStepConfig fetchOnlyMatch = null;
        TransformationStepConfig upsertMaster;

        if (configuration.isEntityMatchEnabled()) {
            fetchOnlyMatch = fetchOnlyMatch(Collections.singletonList(matchStep));
            upsertMaster = mergeMaster(Collections.singletonList(fetchOnlyMatchStep));
        } else {
            upsertMaster = mergeMaster(Collections.singletonList(matchStep));
        }
        TransformationStepConfig diff = diff(mergeStep, upsertMasterStep);
        TransformationStepConfig report = reportDiff(diffStep);

        List<TransformationStepConfig> steps = new ArrayList<>();
        steps.add(merge);
        steps.add(match);
        if (configuration.isEntityMatchEnabled()) {
            steps.add(fetchOnlyMatch);
        }
        steps.add(upsertMaster);
        steps.add(diff);
        steps.add(report);

        return steps;
    }

    private List<TransformationStepConfig> shortCutSteps() {
        TransformationStepConfig report = reportDiff(diffTableName);
        List<TransformationStepConfig> steps = new ArrayList<>();
        steps.add(report);
        return steps;
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
            if (entityKeyMap.getKeyMap().containsKey(MatchKey.SystemId)) {
                // This should not happen because nothing is setting SystemId.
                log.error("SystemId somehow set in KeyMap before MergeAccount!");
            } else {
                // TODO(jwinter): Support other SystemIds in M28.
                // For now, we hard code the SystemID MatchKey and SystemId Priority List to contain only AccountId.
                List<String> systemIdList = Collections.singletonList(InterfaceName.AccountId.toString());
                entityKeyMap.getKeyMap().put(MatchKey.SystemId, systemIdList);
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

        // Prepare Entity Key Map for Fetch Only Match.
        Map<MatchKey, List<String>> keyMap = //
                MatchKeyUtils.resolveKeyMap(Collections.singletonList(InterfaceName.EntityId.name()));
        keyMap.put(MatchKey.EntityId, Collections.singletonList(InterfaceName.EntityId.name()));
        EntityKeyMap entityKeyMap = new EntityKeyMap();
        entityKeyMap.setKeyMap(keyMap);
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
        putStringValueInContext(ACCOUNT_DIFF_TABLE_NAME, diffTableName);
        String batchStoreTableName = dataCollectionProxy.getTableName(customerSpace.toString(), batchStore, inactive);
        putStringValueInContext(ACCOUNT_MASTER_TABLE_NAME, batchStoreTableName);
    }

    @Override
    protected String getBatchStoreName() {
        if (shortCutMode) {
            return batchStoreNameInContext;
        } else {
            return TableUtils.getFullTableName(batchStoreTablePrefix, pipelineVersion);
        }
    }

    protected String getDiffTableName() {
        if (shortCutMode) {
            return diffTableNameInContext;
        } else {
            return TableUtils.getFullTableName(diffTablePrefix, pipelineVersion);
        }
    }

}
