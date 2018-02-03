package com.latticeengines.cdl.workflow.steps.rebuild;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.CEAttr;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_BUCKETER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_PROFILER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_SORTER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_STATS_CALCULATOR;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Value;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.CalculateStatsConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ProfileConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.SorterConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.BaseWrapperStepConfiguration;
import com.latticeengines.proxy.exposed.datacloudapi.TransformationProxy;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformWrapperStep;

public abstract class ProfileStepBase<T extends BaseWrapperStepConfiguration> extends BaseTransformWrapperStep<T> {

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

}
