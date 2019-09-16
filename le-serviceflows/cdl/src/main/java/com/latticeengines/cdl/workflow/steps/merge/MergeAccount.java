package com.latticeengines.cdl.workflow.steps.merge;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_EXTRACT_EMBEDDED_ENTITY;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.config.atlas.ExtractEmbeddedEntityTableConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.serviceflows.workflow.util.ScalingUtils;

@Component(MergeAccount.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MergeAccount extends BaseSingleEntityMergeImports<ProcessAccountStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(MergeAccount.class);

    static final String BEAN_NAME = "mergeAccount";

    private int upsertStep;
    private int diffStep;

    private String diffTableNameInContext;
    private String batchStoreNameInContext;

    private boolean shortCutMode;

    private String matchedAccountTable;
    private String newAccountTableFromContactMatch;
    private String newAccountTableFromTxnMatch;

    @Override
    protected void initializeConfiguration() {
        super.initializeConfiguration();
        List<Table> tablesInCtx = getTableSummariesFromCtxKeys(customerSpace.toString(), //
                Arrays.asList(ACCOUNT_DIFF_TABLE_NAME, ACCOUNT_MASTER_TABLE_NAME));
        shortCutMode = tablesInCtx.stream().noneMatch(Objects::isNull);
        if (shortCutMode) {
            log.info("Found diff table and batch store in context, using short-cut pipeline");
            shortCutMode = true;
            diffTableNameInContext = tablesInCtx.get(0).getName();
            batchStoreNameInContext = tablesInCtx.get(1).getName();
            diffTableName = diffTableNameInContext;
        } else {
            matchedAccountTable = getStringValueFromContext(ENTITY_MATCH_ACCOUNT_TARGETTABLE);
            newAccountTableFromContactMatch = getStringValueFromContext(ENTITY_MATCH_CONTACT_ACCOUNT_TARGETTABLE);
            newAccountTableFromTxnMatch = getStringValueFromContext(ENTITY_MATCH_TXN_ACCOUNT_TARGETTABLE);
            double newTableSize = 0.0D;
            for (String key: Arrays.asList( //
                    ENTITY_MATCH_ACCOUNT_TARGETTABLE, //
                    ENTITY_MATCH_CONTACT_ACCOUNT_TARGETTABLE, //
                    ENTITY_MATCH_TXN_ACCOUNT_TARGETTABLE)) {
                Table tableSummary = getTableSummaryFromKey(customerSpace.toString(), key);
                if (tableSummary != null) {
                    newTableSize += ScalingUtils.getTableSizeInGb(yarnConfiguration, tableSummary);
                }
            }
            double oldTableSize = ScalingUtils.getTableSizeInGb(yarnConfiguration, masterTable);
            scalingMultiplier = ScalingUtils.getMultiplier(oldTableSize + newTableSize);
            log.info(String.format("Adjust scalingMultiplier=%d based on the size of two tables %.2f g.", //
                    scalingMultiplier, oldTableSize + newTableSize));
        }
    }

    @Override
    public PipelineTransformationRequest getConsolidateRequest() {
        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName("MergeAccount");

        if (shortCutMode) {
            request.setSteps(shortCutSteps());
        } else {
            request.setSteps(regularSteps());
        }

        return request;
    }

    private List<TransformationStepConfig> regularSteps() {
        List<TransformationStepConfig> steps;
        if (configuration.isEntityMatchEnabled()) {
            steps = entityMatchSteps();
        } else {
            steps = legacySteps();
        }
        return steps;
    }

    private List<TransformationStepConfig> entityMatchSteps() {
        List<TransformationStepConfig> extracts = new ArrayList<>();
        List<Integer> extractSteps = new ArrayList<>();
        if (StringUtils.isNotBlank(newAccountTableFromContactMatch)) {
            extracts.add(extractNewAccount(newAccountTableFromContactMatch,
                    getStringValueFromContext(ENTITY_MATCH_CONTACT_TARGETTABLE)));
            extractSteps.add(extractSteps.size());
        }
        if (StringUtils.isNotBlank(newAccountTableFromTxnMatch)) {
            extracts.add(extractNewAccount(newAccountTableFromTxnMatch,
                    getStringValueFromContext(ENTITY_MATCH_TXN_TARGETTABLE)));
            extractSteps.add(extractSteps.size());
        }
        List<TransformationStepConfig> steps = new ArrayList<>(extracts);

        int mergeStep = extractSteps.size();
        TransformationStepConfig merge = dedupAndMerge(InterfaceName.EntityId.name(), //
                CollectionUtils.isEmpty(extractSteps) ? null : extractSteps, //
                StringUtils.isBlank(matchedAccountTable) ? null : Collections.singletonList(matchedAccountTable), //
                Collections.singletonList(InterfaceName.CustomerAccountId.name()));
        steps.add(merge);

        upsertStep = mergeStep + 1;
        diffStep = mergeStep + 2;

        TransformationStepConfig upsert = upsertMaster(true, mergeStep);
        TransformationStepConfig diff = diff(mergeStep, upsertStep);
        TransformationStepConfig report = reportDiff(diffStep);
        steps.add(upsert);
        steps.add(diff);
        steps.add(report);

        return steps;
    }

    private List<TransformationStepConfig> legacySteps() {
        List<TransformationStepConfig> steps = new ArrayList<>();

        upsertStep = 0;
        diffStep = 1;
        TransformationStepConfig upsert = upsertMaster(false, matchedAccountTable);
        TransformationStepConfig diff = diff(matchedAccountTable, upsertStep);
        TransformationStepConfig report = reportDiff(diffStep);
        steps.add(upsert);
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

    // for Account batch store
    @Override
    protected void enrichTableSchema(Table table) {
        Map<String, Attribute> attrsToInherit = new HashMap<>();
        addAttrsToMap(attrsToInherit, inputMasterTableName);
        addAttrsToMap(attrsToInherit, matchedAccountTable);
        addAttrsToMap(attrsToInherit, newAccountTableFromContactMatch);
        addAttrsToMap(attrsToInherit, newAccountTableFromTxnMatch);
        updateAttrs(table, attrsToInherit);
        table.getAttributes().forEach(attr -> {
            attr.setTags(Tag.INTERNAL);
            if (configuration.isEntityMatchEnabled() && InterfaceName.AccountId.name().equals(attr.getName())) {
                attr.setDisplayName("Atlas Account ID");
            }
        });
        metadataProxy.updateTable(customerSpace.toString(), table.getName(), table);
    }

    @Override
    protected void onPostTransformationCompleted() {
        super.onPostTransformationCompleted();
        String batchStoreTableName = dataCollectionProxy.getTableName(customerSpace.toString(), batchStore, inactive);
        checkAttributeLimit(batchStoreTableName, configuration.isEntityMatchEnabled());
        exportToS3AndAddToContext(batchStoreTableName, ACCOUNT_MASTER_TABLE_NAME);
        exportToS3AndAddToContext(diffTableName, ACCOUNT_DIFF_TABLE_NAME);
        exportToDynamo(batchStoreTableName, InterfaceName.AccountId.name(), null);
    }

    @Override
    protected String getBatchStoreName() {
        if (shortCutMode) {
            return batchStoreNameInContext;
        } else {
            return TableUtils.getFullTableName(batchStoreTablePrefix, pipelineVersion);
        }
    }

    @Override
    protected String getDiffTableName() {
        if (shortCutMode) {
            return diffTableNameInContext;
        } else {
            return TableUtils.getFullTableName(diffTablePrefix, pipelineVersion);
        }
    }

    private TransformationStepConfig extractNewAccount(String newAccountTable, String matchTargetTable) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(TRANSFORMER_EXTRACT_EMBEDDED_ENTITY);
        addBaseTables(step, newAccountTable);
        addBaseTables(step, matchTargetTable);
        ExtractEmbeddedEntityTableConfig config = new ExtractEmbeddedEntityTableConfig();
        config.setEntity(BusinessEntity.Account.name());
        config.setEntityIdFld(InterfaceName.AccountId.name());
        List<String> systemIds = new ArrayList<>();
        systemIds.add(InterfaceName.CustomerAccountId.name());
        systemIds.addAll(getSystemIds(BusinessEntity.Account));
        // SystemIds which don't exist in match target table are ignored in
        // dataflow
        config.setSystemIdFlds(systemIds);
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        return step;
    }

}
