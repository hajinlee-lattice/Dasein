package com.latticeengines.cdl.workflow.steps.rebuild;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.CEAttr;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_BUCKETER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_PROFILER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_SORTER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_STATS_CALCULATOR;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.CalculateStatsConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ProfileConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.SorterConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.core.steps.RedshiftExportConfig;
import com.latticeengines.domain.exposed.workflow.BaseWrapperStepConfiguration;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.datacloudapi.TransformationProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformWrapperStep;

public abstract class ProfileStepBase<T extends BaseWrapperStepConfiguration> extends BaseTransformWrapperStep<T> {

    private static final Logger log = LoggerFactory.getLogger(ProfileStepBase.class);

    @Inject
    protected DataCollectionProxy dataCollectionProxy;

    @Inject
    protected MetadataProxy metadataProxy;

    @Inject
    protected TransformationProxy transformationProxy;

    @Value("${dataplatform.queue.scheme}")
    protected String queueScheme;

    protected CustomerSpace customerSpace;

    protected <V> void updateEntityValueMapInContext(String key, V value, Class<V> clz) {
        updateEntityValueMapInContext(getEntity(), key, value, clz);
    }

    protected <V> void updateEntityValueMapInContext(BusinessEntity entity, String key, V value, Class<V> clz) {
        Map<BusinessEntity, V> entityValueMap = getMapObjectFromContext(key, BusinessEntity.class, clz);
        if (entityValueMap == null) {
            entityValueMap = new HashMap<>();
        }
        entityValueMap.put(entity, value);
        putObjectInContext(key, entityValueMap);
    }

    protected TransformationStepConfig profile(String masterTableName) {
        TransformationStepConfig step = initStepWithInputTable(masterTableName, "CustomerUniverse");
        return configureProfileStep(step);
    }

    protected TransformationStepConfig profile(int inputStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(inputStep));
        return configureProfileStep(step);
    }

    private TransformationStepConfig configureProfileStep(TransformationStepConfig step) {
        step.setTransformer(TRANSFORMER_PROFILER);
        ProfileConfig conf = new ProfileConfig();
        conf.setEncAttrPrefix(CEAttr);
        String confStr = appendEngineConf(conf, lightEngineConfig());
        step.setConfiguration(confStr);
        return step;
    }

    protected TransformationStepConfig bucket(int profileStep, String masterTableName) {
        TransformationStepConfig step = initStepWithInputTable(masterTableName, "CustomerUniverse");
        step.setInputSteps(Collections.singletonList(profileStep));
        return configureBucketStep(step);
    }

    protected TransformationStepConfig bucket(int profileStep, int inputStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Arrays.asList(profileStep, inputStep));
        return configureBucketStep(step);
    }

    private TransformationStepConfig configureBucketStep(TransformationStepConfig step) {
        step.setTransformer(TRANSFORMER_BUCKETER);
        step.setConfiguration(emptyStepConfig(lightEngineConfig()));
        return step;
    }

    protected TransformationStepConfig calcStats(int profileStep, int bucketStep, String statsTablePrefix,
            List<String> dedupFields) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Arrays.asList(bucketStep, profileStep));
        step.setTransformer(TRANSFORMER_STATS_CALCULATOR);

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(statsTablePrefix);
        step.setTargetTable(targetTable);

        CalculateStatsConfig conf = new CalculateStatsConfig();
        if (CollectionUtils.isNotEmpty(dedupFields)) {
            conf.setDedupFields(dedupFields);
        }
        step.setConfiguration(appendEngineConf(conf, lightEngineConfig()));
        return step;
    }

    protected TransformationStepConfig sort(String inputTableName, String outputTablePrefix, String sortKey,
            int partitions) {
        TransformationStepConfig step = initStepWithInputTable(inputTableName, "Contacts");
        return configSortStep(step, outputTablePrefix, sortKey, partitions);
    }

    protected TransformationStepConfig sort(int inputStep, String outputTablePrefix, String sortKey, int partitions) {
        TransformationStepConfig step = new TransformationStepConfig();
        List<Integer> inputSteps = Collections.singletonList(inputStep);
        step.setInputSteps(inputSteps);
        return configSortStep(step, outputTablePrefix, sortKey, partitions);
    }

    private TransformationStepConfig configSortStep(TransformationStepConfig step, String outputTablePrefix,
            String sortKey, int partitions) {
        step.setTransformer(TRANSFORMER_SORTER);

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(outputTablePrefix);
        targetTable.setExpandBucketedAttrs(true);
        step.setTargetTable(targetTable);

        SorterConfig conf = new SorterConfig();
        conf.setPartitions(partitions);
        if (partitions > 1) {
            conf.setSplittingThreads(maxSplitThreads);
            conf.setSplittingChunkSize(10000L);
        }
        conf.setCompressResult(true);
        // TODO: only support single sort key now
        conf.setSortingField(sortKey);
        String confStr = appendEngineConf(conf, lightEngineConfig());
        step.setConfiguration(confStr);
        return step;
    }

    private TransformationStepConfig initStepWithInputTable(String inputTableName, String tableSourceName) {
        TransformationStepConfig step = new TransformationStepConfig();
        SourceTable sourceTable = new SourceTable(inputTableName, customerSpace);
        List<String> baseSources = Collections.singletonList(tableSourceName);
        step.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableSourceName, sourceTable);
        step.setBaseTables(baseTables);
        return step;
    }

    protected abstract BusinessEntity getEntity();

    protected String renameServingStoreTable(Table servingStoreTable) {
        return renameServingStoreTable(getEntity(), servingStoreTable);
    }

    protected String renameServingStoreTable(BusinessEntity servingEntity, Table servingStoreTable) {
        String prefix = String.join("_", customerSpace.getTenantId(), servingEntity.name());
        String goodName = NamingUtils.timestamp(prefix);
        log.info("Renaming table " + servingStoreTable.getName() + " to " + goodName);
        metadataProxy.updateTable(customerSpace.toString(), goodName, servingStoreTable);
        servingStoreTable.setName(goodName);
        return goodName;
    }

    protected RedshiftExportConfig exportTableRole(String tableName, TableRoleInCollection tableRole) {
        String distKey = tableRole.getPrimaryKey().name();
        List<String> sortKeys = new ArrayList<>(tableRole.getForeignKeysAsStringList());
        if (!sortKeys.contains(tableRole.getPrimaryKey().name())) {
            sortKeys.add(tableRole.getPrimaryKey().name());
        }
        RedshiftExportConfig config = new RedshiftExportConfig();
        config.setTableName(tableName);
        config.setDistKey(distKey);
        config.setSortKeys(sortKeys);
        config.setInputPath(getInputPath(tableName) + "/*.avro");
        config.setUpdateMode(false);
        return config;
    }

    private String getInputPath(String tableName) {
        Table table = metadataProxy.getTable(configuration.getCustomerSpace().toString(), tableName);
        if (table == null) {
            throw new IllegalArgumentException("Cannot find table named " + tableName);
        }
        List<Extract> extracts = table.getExtracts();
        if (CollectionUtils.isEmpty(extracts) || extracts.size() != 1) {
            throw new IllegalArgumentException("Table " + tableName + " does not have single extract");
        }
        Extract extract = extracts.get(0);
        String path = extract.getPath();
        if (path.endsWith(".avro") || path.endsWith("/")) {
            path = path.substring(0, path.lastIndexOf("/"));
        }
        return path;
    }

}
