package com.latticeengines.cdl.workflow.steps.update;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_BUCKETER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_CONSOLIDATE_RETAIN;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MATCH;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_SORTER;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.UnionSelection;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidateRetainFieldConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.MatchTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.SorterConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.BaseProcessEntityStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformWrapperStep;

public abstract class BaseProcessSingleEntityDiffStep<T extends BaseProcessEntityStepConfiguration>
        extends BaseTransformWrapperStep<T> {

    protected CustomerSpace customerSpace;
    protected DataCollection.Version active;
    protected DataCollection.Version inactive;

    protected BusinessEntity entity;

    private String sortedTablePrefix;
    private String diffTableName;
    private String profileTableName;

    private TableRoleInCollection servingStore;
    private String servingStorePrimaryKey;
    private List<String> servingStoreSortKeys;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private MetadataProxy metadataProxy;

    @Override
    protected TransformationWorkflowConfiguration executePreTransformation() {
        initializeConfiguration();
        return generateWorkflowConf();
    }

    @Override
    protected void onPostTransformationCompleted() {
        String redshiftTableName = TableUtils.getFullTableName(sortedTablePrefix, pipelineVersion);
        Table redshiftTable = metadataProxy.getTable(customerSpace.toString(), redshiftTableName);
        if (redshiftTable == null) {
            throw new RuntimeException("Diff table has not been created.");
        }
        Map<BusinessEntity, String> entityTableMap = getMapObjectFromContext(TABLE_GOING_TO_REDSHIFT,
                BusinessEntity.class, String.class);
        if (entityTableMap == null) {
            entityTableMap = new HashMap<>();
        }
        entityTableMap.put(entity, redshiftTableName);
        putObjectInContext(TABLE_GOING_TO_REDSHIFT, entityTableMap);

        Map<BusinessEntity, Boolean> appendTableMap = getMapObjectFromContext(APPEND_TO_REDSHIFT_TABLE,
                BusinessEntity.class, Boolean.class);
        if (appendTableMap == null) {
            appendTableMap = new HashMap<>();
        }
        appendTableMap.put(entity, true);
        putObjectInContext(APPEND_TO_REDSHIFT_TABLE, appendTableMap);
    }

    private void initializeConfiguration() {
        customerSpace = configuration.getCustomerSpace();
        active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
        entity = configuration.getMainEntity();

        servingStore = entity.getServingStore();
        if (servingStore != null) {
            servingStorePrimaryKey = servingStore.getPrimaryKey().name();
            servingStoreSortKeys = servingStore.getForeignKeysAsStringList();
        }

        Map<BusinessEntity, String> diffTableNames = getMapObjectFromContext(ENTITY_DIFF_TABLES,
                BusinessEntity.class, String.class);
        diffTableName = diffTableNames.get(entity);

        profileTableName = dataCollectionProxy.getTableName(customerSpace.toString(), profileTableRole(), inactive);
        sortedTablePrefix = entity.name() + "_Diff_Sorted";
    }

    private TransformationWorkflowConfiguration generateWorkflowConf() {
        PipelineTransformationRequest request = getTransformRequest();
        return transformationProxy.getWorkflowConf(request, configuration.getPodId());
    }

    protected TransformationStepConfig match() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(TRANSFORMER_MATCH);

        useDiffTableAsSource(step);

        // Match Input
        MatchTransformerConfig config = new MatchTransformerConfig();
        MatchInput matchInput = new MatchInput();
        matchInput.setTenant(new Tenant(customerSpace.toString()));
        UnionSelection us = new UnionSelection();
        Map<ColumnSelection.Predefined, String> ps = new HashMap<>();
        ps.put(ColumnSelection.Predefined.Segment, "2.0");
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

    protected TransformationStepConfig bucket(boolean heavyEngine) {
        return bucket(-1, heavyEngine);
    }

    protected TransformationStepConfig bucket(int inputStep, boolean heavyEngine) {
        TransformationStepConfig step = new TransformationStepConfig();
        String sourceName = entity.name() + "Profile";
        SourceTable sourceTable = new SourceTable(profileTableName, customerSpace);
        List<String> baseSources = Collections.singletonList(sourceName);
        step.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(sourceName, sourceTable);
        step.setBaseTables(baseTables);
        if (inputStep < 0) {
            useDiffTableAsSource(step);
        } else {
            step.setInputSteps(Collections.singletonList(inputStep));
        }
        step.setTransformer(TRANSFORMER_BUCKETER);
        String confStr = heavyEngine ? emptyStepConfig(heavyEngineConfig()) : emptyStepConfig(lightEngineConfig());
        step.setConfiguration(confStr);
        return step;
    }

    protected TransformationStepConfig retainFields(int previousStep, boolean useTargetTable) {
        return retainFields(previousStep, useTargetTable, servingStore);
    }

    protected TransformationStepConfig retainFields(int previousStep, boolean useTargetTable, TableRoleInCollection role) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(previousStep));
        step.setTransformer(TRANSFORMER_CONSOLIDATE_RETAIN);

        if (useTargetTable) {
            TargetTable targetTable = new TargetTable();
            targetTable.setCustomerSpace(customerSpace);
            targetTable.setNamePrefix(sortedTablePrefix);
            targetTable.setPrimaryKey(servingStorePrimaryKey);
            targetTable.setExpandBucketedAttrs(true);
            step.setTargetTable(targetTable);
        }

        ConsolidateRetainFieldConfig config = new ConsolidateRetainFieldConfig();
        Table servingTable = dataCollectionProxy.getTable(customerSpace.toString(), role);
        if (servingTable != null) {
            List<String> fieldsToRetain = AvroUtils.getSchemaFields(yarnConfiguration,
                    servingTable.getExtracts().get(0).getPath());
            config.setFieldsToRetain(fieldsToRetain);
        }
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        return step;
    }

    protected TransformationStepConfig sort(int diffStep, int partitions) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(diffStep));
        step.setTransformer(TRANSFORMER_SORTER);

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(sortedTablePrefix);
        targetTable.setPrimaryKey(servingStorePrimaryKey);
        targetTable.setExpandBucketedAttrs(true);
        step.setTargetTable(targetTable);

        SorterConfig config = new SorterConfig();
        config.setPartitions(partitions);
        String sortingKey = servingStorePrimaryKey;
        if (!servingStoreSortKeys.isEmpty()) {
            sortingKey = servingStoreSortKeys.get(0);
        }
        config.setSortingField(sortingKey);
        config.setCompressResult(true);
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));

        return step;
    }

    private void useDiffTableAsSource(TransformationStepConfig step) {
        String sourceName = entity.name() + "Diff";
        SourceTable sourceTable = new SourceTable(diffTableName, customerSpace);
        List<String> baseSources = step.getBaseSources();
        if (CollectionUtils.isEmpty(baseSources)) {
            baseSources = Collections.singletonList(sourceName);
        } else {
            baseSources.add(sourceName);
        }
        step.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = step.getBaseTables();
        if (MapUtils.isEmpty(baseTables)) {
            baseTables = new HashMap<>();
        }
        baseTables.put(sourceName, sourceTable);
        step.setBaseTables(baseTables);
    }

    protected abstract PipelineTransformationRequest getTransformRequest();

    protected abstract TableRoleInCollection profileTableRole();

}
