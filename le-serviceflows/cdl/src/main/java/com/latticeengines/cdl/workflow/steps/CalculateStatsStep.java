package com.latticeengines.cdl.workflow.steps;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_BUCKETED_FILTER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_BUCKETER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MATCH;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_PROFILER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_SORTER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_STATS_CALCULATOR;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.BucketedFilterConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.MatchTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ProfileConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.SorterConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.CalculateStatsStepConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.datacloudapi.TransformationProxy;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationStep;

@Component("calculateStatsStep")
public class CalculateStatsStep extends BaseTransformationStep<CalculateStatsStepConfiguration> {

    private static final Log log = LogFactory.getLog(CalculateStatsStep.class);

    private static final String PROFILE_TABLE_PREFIX = "Profile";
    private static final String STATS_TABLE_PREFIX = "Stats";
    private static final String SORTED_TABLE_PREFIX = "Sorted";

    private static int matchStep;
    private static int profileStep;
    private static int bucketStep;
    private static int filterStep;

    @Autowired
    private TransformationProxy transformationProxy;

    @Autowired
    private DataCollectionProxy dataCollectionProxy;

    @Autowired
    private MetadataProxy metadataProxy;

    private String pipelineVersion;

    @Override
    public void execute() {
        log.info("Inside CalculateStats execute()");

        String customerSpace = configuration.getCustomerSpace().toString();
        String collectionName = configuration.getDataCollectionName();

        Table masterTable = dataCollectionProxy.getTable(customerSpace, collectionName,
                TableRoleInCollection.ConsolidatedAccount);
        if (masterTable == null) {
            throw new IllegalStateException("Cannot find the master table in collection " + collectionName);
        }
        log.info(String.format("masterTableName for customer %s is %s", configuration.getCustomerSpace().toString(),
                masterTable.getName()));

        PipelineTransformationRequest request = generateRequest(configuration.getCustomerSpace(), masterTable);
        TransformationProgress progress = transformationProxy.transform(request, "");
        waitForFinish(progress);

        pipelineVersion = progress.getVersion();
        log.info(String.format("The pipeline version for customer %s is %s",
                configuration.getCustomerSpace().toString(), pipelineVersion));
    }


    @Override
    public void onExecutionCompleted() {
        String profileTableName = TableUtils.getFullTableName(PROFILE_TABLE_PREFIX, pipelineVersion);
        String statsTableName = TableUtils.getFullTableName(STATS_TABLE_PREFIX, pipelineVersion);
        String sortedTableName = TableUtils.getFullTableName(SORTED_TABLE_PREFIX, pipelineVersion);
        putStringValueInContext(CALCULATE_STATS_TARGET_TABLE, statsTableName);
        putObjectInContext(SPLIT_LOCAL_FILE_FOR_REDSHIFT, Boolean.FALSE);
        upsertTables(configuration.getCustomerSpace().toString(), profileTableName, sortedTableName);

        Table sortedTable = metadataProxy.getTable(configuration.getCustomerSpace().toString(), sortedTableName);
        putObjectInContext(TABLE_GOING_TO_REDSHIFT, sortedTable);
    }

    private PipelineTransformationRequest generateRequest(CustomerSpace customerSpace, Table masterTable) {
        String masterTableName = masterTable.getName();
        List<String> originalAttrs = Arrays.asList(masterTable.getAttributeNames());
        try {
            PipelineTransformationRequest request = new PipelineTransformationRequest();
            request.setName("CalculateStatsStep");
            request.setSubmitter(customerSpace.getTenantId());
            request.setKeepTemp(false);
            request.setEnableSlack(false);
            matchStep = 0;
            profileStep = 1;
            bucketStep = 2;
            filterStep = 4;
            // -----------
            TransformationStepConfig match = match(customerSpace, masterTableName);
            TransformationStepConfig profile = profile();
            TransformationStepConfig bucket = bucket();
            TransformationStepConfig calc = calcStats(customerSpace, STATS_TABLE_PREFIX);
            TransformationStepConfig filter = filter(originalAttrs);
            TransformationStepConfig sort = sort(customerSpace);
            TransformationStepConfig sortProfile = sortProfile(customerSpace, PROFILE_TABLE_PREFIX);
            // -----------
            List<TransformationStepConfig> steps = Arrays.asList( //
                    match, //
                    profile, //
                    bucket, //
                    calc, //
                    filter, //
                    sort, //
                    sortProfile //
            );
            // -----------
            request.setSteps(steps);
            request.setContainerMemMB(workflowMemMbMax);
            return request;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private TransformationStepConfig match(CustomerSpace customerSpace, String sourceTableName) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(TRANSFORMER_MATCH);

        String tableSourceName = "CustomerUniverse";
        SourceTable sourceTable = new SourceTable(sourceTableName, customerSpace);
        List<String> baseSources = Collections.singletonList(tableSourceName);
        step.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableSourceName, sourceTable);
        step.setBaseTables(baseTables);

        MatchTransformerConfig config = new MatchTransformerConfig();
        MatchInput matchInput = new MatchInput();
        matchInput.setTenant(new Tenant(customerSpace.toString()));
        matchInput.setPredefinedSelection(ColumnSelection.Predefined.Segment);
        matchInput.setKeyMap(getKeyMap());
        matchInput.setDataCloudVersion(getDataCloudVersion());
        matchInput.setSkipKeyResolution(true);
        matchInput.setFetchOnly(true);
        config.setMatchInput(matchInput);
        step.setConfiguration(JsonUtils.serialize(config));

        return step;
    }

    private TransformationStepConfig profile() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(matchStep));
        step.setTransformer(TRANSFORMER_PROFILER);
        ProfileConfig conf = new ProfileConfig();
        String confStr = appendEngineConf(conf, heavyEngineConfig());
        step.setConfiguration(confStr);
        return step;
    }

    private TransformationStepConfig bucket() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Arrays.asList(matchStep, profileStep));
        step.setTransformer(TRANSFORMER_BUCKETER);
        step.setConfiguration(emptyStepConfig(heavyEngineConfig()));
        return step;
    }

    private TransformationStepConfig calcStats(CustomerSpace customerSpace, String statsTablePrefix) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Arrays.asList(bucketStep, profileStep));
        step.setTransformer(TRANSFORMER_STATS_CALCULATOR);

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(statsTablePrefix);
        step.setTargetTable(targetTable);

        step.setConfiguration(emptyStepConfig(heavyEngineConfig()));
        return step;
    }

    private TransformationStepConfig filter(List<String> originalAttrs) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(bucketStep));
        step.setTransformer(TRANSFORMER_BUCKETED_FILTER);
        BucketedFilterConfig conf = new BucketedFilterConfig();
        conf.setOriginalAttrs(originalAttrs);
        String confStr = appendEngineConf(conf, heavyEngineConfig());
        step.setConfiguration(confStr);
        return step;
    }

    private TransformationStepConfig sort(CustomerSpace customerSpace) {
        TransformationStepConfig step = new TransformationStepConfig();
        List<Integer> inputSteps = Collections.singletonList(filterStep);
        step.setInputSteps(inputSteps);
        step.setTransformer(TRANSFORMER_SORTER);

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(SORTED_TABLE_PREFIX);
        step.setTargetTable(targetTable);

        SorterConfig conf = new SorterConfig();
        conf.setPartitions(500);
        conf.setSplittingThreads(maxSplitThreads);
        conf.setCompressResult(false);
        conf.setSortingField(InterfaceName.LatticeAccountId.name());
        String confStr = appendEngineConf(conf, lightEngineConfig());
        step.setConfiguration(confStr);
        return step;
    }

    private TransformationStepConfig sortProfile(CustomerSpace customerSpace, String profileTablePrefix) {
        TransformationStepConfig step = new TransformationStepConfig();
        List<Integer> inputSteps = Collections.singletonList(profileStep);
        step.setInputSteps(inputSteps);
        step.setTransformer(TRANSFORMER_SORTER);

        SorterConfig conf = new SorterConfig();
        conf.setPartitions(1);
        conf.setCompressResult(true);
        conf.setSortingField(DataCloudConstants.PROFILE_ATTR_ATTRNAME);
        String confStr = appendEngineConf(conf, lightEngineConfig());
        step.setConfiguration(confStr);

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(profileTablePrefix);
        step.setTargetTable(targetTable);

        return step;
    }

    private Map<MatchKey, List<String>> getKeyMap() {
        Map<MatchKey, List<String>> keyMap = new TreeMap<>();
        keyMap.put(MatchKey.LatticeAccountID, Collections.singletonList(InterfaceName.LatticeAccountId.name()));
        return keyMap;
    }

    private void upsertTables(String customerSpace, String profileTableName, String sortedTableName) {
        Table profileTable = metadataProxy.getTable(customerSpace, profileTableName);
        if (profileTable == null) {
            throw new RuntimeException("Failed to find profile table in customer " + customerSpace);
        }
        String collectionName = configuration.getDataCollectionName();
        dataCollectionProxy.upsertTable(customerSpace, collectionName, profileTableName, TableRoleInCollection.Profile);
        profileTable = dataCollectionProxy.getTable(customerSpace, collectionName, TableRoleInCollection.Profile);
        if (profileTable == null) {
            throw new IllegalStateException("Cannot find the upserted profile table in data collection.");
        }

        Table bktTable = metadataProxy.getTable(customerSpace, sortedTableName);
        if (bktTable == null) {
            throw new RuntimeException("Failed to find bucketed table in customer " + customerSpace);
        }
        dataCollectionProxy.upsertTable(customerSpace, collectionName, sortedTableName, TableRoleInCollection.BucketedAccount);
        bktTable = dataCollectionProxy.getTable(customerSpace, collectionName, TableRoleInCollection.BucketedAccount);
        if (bktTable == null) {
            throw new IllegalStateException("Cannot find the upserted bucketed table in data collection.");
        }
    }

}