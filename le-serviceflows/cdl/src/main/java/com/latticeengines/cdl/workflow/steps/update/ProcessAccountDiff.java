package com.latticeengines.cdl.workflow.steps.update;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_CONSOLIDATE_DATA;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_COPIER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MATCH;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.BulkMatchMergerTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidateDataTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.CopierConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.MatchTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;

@Component(ProcessAccountDiff.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ProcessAccountDiff extends BaseProcessSingleEntityDiffStep<ProcessAccountStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ProcessAccountDiff.class);

    static final String BEAN_NAME = "processAccountDiff";

    @Inject
    private ColumnMetadataProxy columnMetadataProxy;

    @Inject
    private ServingStoreProxy servingStoreProxy;

    protected String diffSlimTableName;
    private Table accountFeatureTable;
    private String accountFeatureTableName;

    private String filterAccountFeatureTablePrefix = "FilterAccountFeatures";

    private String mergeAccountFeatureTablePrefix = "MergeAccountFeatures";

    @Override
    protected PipelineTransformationRequest getTransformRequest() {
        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName("ConsolidateAccountDiff");

        int step = 0;
        int matchStep = step++;
        int mergeMatchStep = -1;
        if (diffSlimTableName != null)
            mergeMatchStep = step++;
        int bucketStep = step++;
        int retainStep = step++;
        int sortStep = step++;
        int filterAccountFeatureStep = step++;

        TransformationStepConfig matchDiff = match();
        TransformationStepConfig mergeMatch = diffSlimTableName != null ? mergeMatch(matchStep) : null;
        int newMatchStep = diffSlimTableName != null ? mergeMatchStep : matchStep;
        TransformationStepConfig bucket = bucket(newMatchStep, true);
        TransformationStepConfig retainFields = retainFields(bucketStep, false);
        TransformationStepConfig sort = sort(retainStep, 200);
        TransformationStepConfig filterAccountFeature = filterAccountFeature(newMatchStep, customerSpace,
                filterAccountFeatureTablePrefix);
        TransformationStepConfig mergeAccountFeature = mergeAccountFeatures(filterAccountFeatureStep,
                mergeAccountFeatureTablePrefix);

        List<TransformationStepConfig> steps = new ArrayList<>();
        steps.add(matchDiff);
        if (mergeMatch != null)
            steps.add(mergeMatch);
        steps.add(bucket);
        steps.add(retainFields);
        steps.add(sort);
        steps.add(filterAccountFeature);
        steps.add(mergeAccountFeature);
        request.setSteps(steps);
        return request;
    }

    @Override
    protected TableRoleInCollection profileTableRole() {
        return TableRoleInCollection.Profile;
    }

    @Override
    protected void initializeConfiguration() {
        super.initializeConfiguration();
        accountFeatureTable = dataCollectionProxy.getTable(customerSpace.toString(),
                TableRoleInCollection.AccountFeatures, active);
        if (accountFeatureTable == null || accountFeatureTable.getExtracts().isEmpty()) {
            log.info("There has been no Account Feature table!");
        } else {
            accountFeatureTableName = accountFeatureTable.getName();
        }
        log.info("Set accountFeatureTableName=" + accountFeatureTableName);

        diffSlimTableName = dataCollectionProxy.getTableName(customerSpace.toString(),
                TableRoleInCollection.AccountDiffSlim, inactive);

    }

    @Override
    protected void onPostTransformationCompleted() {
        super.onPostTransformationCompleted();
        createAccountFeatures();
        registerDynamoExport();
    }

    private TransformationStepConfig match() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(TRANSFORMER_MATCH);

        useDiffTableAsSource(step, diffSlimTableName != null ? diffSlimTableName : diffTableName);

        // Match Input
        MatchTransformerConfig config = new MatchTransformerConfig();
        MatchInput matchInput = new MatchInput();
        matchInput.setTenant(new Tenant(customerSpace.toString()));

        String dataCloudVersion = "";
        DataCollectionStatus detail = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        if (detail != null) {
            try {
                dataCloudVersion = DataCloudVersion.parseBuildNumber(detail.getDataCloudBuildNumber()).getVersion();
            } catch (Exception e) {
                log.warn("Failed to read datacloud version from collection status " + JsonUtils.serialize(detail));
            }
        }

        List<ColumnMetadata> dcCols = columnMetadataProxy.getAllColumns(dataCloudVersion);
        List<Column> cols = new ArrayList<>();
        for (ColumnMetadata cm : dcCols) {
            cols.add(new Column(cm.getAttrName()));
        }
        ColumnSelection cs = new ColumnSelection();
        cs.setColumns(cols);
        matchInput.setCustomSelection(cs);

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

    private TransformationStepConfig mergeMatch(int matchStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        List<Integer> steps = Arrays.asList(matchStep);
        step.setInputSteps(steps);
        useDiffTableAsSource(step, diffTableName);
        step.setTransformer("bulkMatchMergerTransformer");

        BulkMatchMergerTransformerConfig conf = new BulkMatchMergerTransformerConfig();
        conf.setJoinField(InterfaceName.AccountId.name());
        conf.setReverse(true);
        String confStr = appendEngineConf(conf, heavyEngineConfig2());
        step.setConfiguration(confStr);
        return step;
    }

    private TransformationStepConfig filterAccountFeature(int matchStep, CustomerSpace customerSpace,
            String accountFeaturesTablePrefix) {
        TransformationStepConfig step = new TransformationStepConfig();
        List<Integer> inputSteps = Collections.singletonList(matchStep);
        step.setInputSteps(inputSteps);
        step.setTransformer(TRANSFORMER_COPIER);

        List<String> retainAttrNames = servingStoreProxy //
                .getAllowedModelingAttrs(customerSpace.toString(), true, inactive) //
                .map(ColumnMetadata::getAttrName) //
                .collectList().block();
        if (retainAttrNames == null) {
            retainAttrNames = new ArrayList<>();
        }
        log.info(String.format("retainAttrNames from servingStore: %d", retainAttrNames.size()));
        if (!retainAttrNames.contains(InterfaceName.AccountId.name())) {
            retainAttrNames.add(InterfaceName.AccountId.name());
        }
        if (!retainAttrNames.contains(InterfaceName.LatticeAccountId.name())) {
            retainAttrNames.add(InterfaceName.LatticeAccountId.name());
        }

        CopierConfig conf = new CopierConfig();
        conf.setRetainAttrs(retainAttrNames);
        String confStr = appendEngineConf(conf, heavyEngineConfig());
        step.setConfiguration(confStr);

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(accountFeaturesTablePrefix);
        step.setTargetTable(targetTable);

        return step;
    }

    private TransformationStepConfig mergeAccountFeatures(int filterAccountFeatureStep,
            String accountFeaturesTablePrefix) {
        TransformationStepConfig step = new TransformationStepConfig();
        setupMasterTable(step);
        step.setInputSteps(Collections.singletonList(filterAccountFeatureStep));
        step.setTransformer(TRANSFORMER_CONSOLIDATE_DATA);
        step.setConfiguration(getMergeMasterConfig());

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(accountFeaturesTablePrefix);
        step.setTargetTable(targetTable);
        return step;
    }

    private String getMergeMasterConfig() {
        ConsolidateDataTransformerConfig config = new ConsolidateDataTransformerConfig();
        config.setSrcIdField(InterfaceName.AccountId.name());
        config.setMasterIdField(InterfaceName.AccountId.name());
        return appendEngineConf(config, heavyEngineConfig());
    }

    private void setupMasterTable(TransformationStepConfig step) {
        List<String> baseSources;
        Map<String, SourceTable> baseTables;
        if (StringUtils.isNotBlank(accountFeatureTableName)) {
            Table masterTable = metadataProxy.getTable(customerSpace.toString(), accountFeatureTableName);
            if (masterTable != null && !masterTable.getExtracts().isEmpty()) {
                baseSources = Collections.singletonList(accountFeatureTableName);
                baseTables = new HashMap<>();
                SourceTable sourceMasterTable = new SourceTable(accountFeatureTableName, customerSpace);
                baseTables.put(accountFeatureTableName, sourceMasterTable);
                step.setBaseSources(baseSources);
                step.setBaseTables(baseTables);
            }
        }
    }

    private void createAccountFeatures() {
        String customerSpace = configuration.getCustomerSpace().toString();
        String accountFeatureTableName = TableUtils.getFullTableName(mergeAccountFeatureTablePrefix, pipelineVersion);
        Table accountFeatureTable = metadataProxy.getTable(customerSpace, accountFeatureTableName);
        if (accountFeatureTable == null) {
            throw new RuntimeException(
                    "Failed to find accountFeature table " + accountFeatureTableName + " in customer " + customerSpace);
        }
        setAccountFeatureTableSchema(accountFeatureTable);
        DataCollection.Version inactiveVersion = dataCollectionProxy.getInactiveVersion(customerSpace);
        dataCollectionProxy.upsertTable(customerSpace, accountFeatureTableName, TableRoleInCollection.AccountFeatures,
                inactiveVersion);
        accountFeatureTable = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.AccountFeatures,
                inactiveVersion);
        if (accountFeatureTable == null) {
            throw new IllegalStateException(
                    "Cannot find the upserted " + TableRoleInCollection.AccountFeatures + " table in data collection.");
        }
    }

    private void setAccountFeatureTableSchema(Table table) {
        String dataCloudVersion = configuration.getDataCloudVersion();
        List<ColumnMetadata> amCols = columnMetadataProxy.columnSelection(ColumnSelection.Predefined.Model,
                dataCloudVersion);
        Map<String, ColumnMetadata> amColMap = new HashMap<>();
        amCols.forEach(cm -> amColMap.put(cm.getAttrName(), cm));
        List<Attribute> attrs = new ArrayList<>();
        table.getAttributes().forEach(attr0 -> {
            Attribute attr = attr0;
            if (amColMap.containsKey(attr0.getName())) {
                ColumnMetadata cm = amColMap.get(attr0.getName());
                if (Category.ACCOUNT_ATTRIBUTES.equals(cm.getCategory())) {
                    attr.setTags(Tag.INTERNAL);
                }
            }
            attrs.add(attr);
        });
        table.setAttributes(attrs);
        metadataProxy.updateTable(customerSpace.toString(), table.getName(), table);
    }

    private void registerDynamoExport() {
        String masterTableName = dataCollectionProxy.getTableName(customerSpace.toString(),
                TableRoleInCollection.ConsolidatedAccount, inactive);
        exportToDynamo(diffTableName, masterTableName, InterfaceName.AccountId.name(), null);
    }
}
