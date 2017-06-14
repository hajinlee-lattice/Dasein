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
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
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

    private static final int ENRICH_STEP = 0;
    private static final int PROFILE_STEP = 1;
    private static final int BUCKET_STEP = 2;
    private static final int FILTER_STEP = 4;

    @Autowired
    private TransformationProxy transformationProxy;

    @Autowired
    private DataCollectionProxy dataCollectionProxy;

    @Autowired
    private MetadataProxy metadataProxy;

    @Override
    public void execute() {
        log.info("Inside CalculateStats execute()");
        String customerSpace = configuration.getCustomerSpace().toString();
        DataCollectionType dataCollectionType = configuration.getDataCollectionType();

        DataCollection dataCollection = dataCollectionProxy.getDataCollectionByType(customerSpace, dataCollectionType);
        Table masterTable = CDLWorkflowStepUtils.getMasterTable(dataCollection);
        log.info(String.format("masterTableName for customer %s is %s", configuration.getCustomerSpace().toString(),
                masterTable.getName()));

        PipelineTransformationRequest request = generateRequest(configuration.getCustomerSpace(), masterTable);
        TransformationProgress progress = transformationProxy.transform(request, "");
        waitForFinish(progress);

        String version = progress.getVersion();
        log.info(String.format("The pipeline version for customer %s is %s",
                configuration.getCustomerSpace().toString(), version));

        String profileTableName = TableUtils.getFullTableName(PROFILE_TABLE_PREFIX, version);
        String statsTableName = TableUtils.getFullTableName(STATS_TABLE_PREFIX, version);
        String sortedTableName = TableUtils.getFullTableName(SORTED_TABLE_PREFIX, version);
        putStringValueInContext(CALCULATE_STATS_TARGET_TABLE, statsTableName);
        putStringValueInContext(CALCULATE_STATS_SORTED_TABLE, sortedTableName);

        setProfileTable(configuration.getCustomerSpace().toString(), profileTableName);
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
            // -----------
            TransformationStepConfig enrich = match(customerSpace, masterTableName);
            TransformationStepConfig profile = profile(customerSpace, PROFILE_TABLE_PREFIX);
            TransformationStepConfig bucket = bucket();
            TransformationStepConfig calc = calcStats(customerSpace, STATS_TABLE_PREFIX);
            TransformationStepConfig filter = filter(originalAttrs);
            TransformationStepConfig sort = sort(customerSpace);
            // -----------
            List<TransformationStepConfig> steps = Arrays.asList( //
                    enrich, //
                    profile, //
                    bucket, //
                    calc, //
                    filter, //
                    sort //
            );
            // -----------
            request.setSteps(steps);
            request.setContainerMemMB(workflowMemMb);
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

    private TransformationStepConfig profile(CustomerSpace customerSpace, String profileTablePrefix) {
        TransformationStepConfig step = new TransformationStepConfig();
        List<Integer> inputSteps = Collections.singletonList(ENRICH_STEP);
        step.setInputSteps(inputSteps);
        step.setTransformer(TRANSFORMER_PROFILER);

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(profileTablePrefix);
        step.setTargetTable(targetTable);

        ProfileConfig conf = new ProfileConfig();
        String confStr = appendEngineConf(conf, baseEngineConfig());
        step.setConfiguration(confStr);
        return step;
    }

    private TransformationStepConfig bucket() {
        TransformationStepConfig step = new TransformationStepConfig();
        List<Integer> inputSteps = Arrays.asList(ENRICH_STEP, PROFILE_STEP);
        step.setInputSteps(inputSteps);
        step.setTransformer(TRANSFORMER_BUCKETER);
        step.setConfiguration(emptyStepConfig());
        return step;
    }

    private TransformationStepConfig calcStats(CustomerSpace customerSpace, String statsTablePrefix) {
        TransformationStepConfig step = new TransformationStepConfig();

        List<Integer> inputSteps = Arrays.asList(BUCKET_STEP, PROFILE_STEP);
        step.setInputSteps(inputSteps);
        step.setTransformer(TRANSFORMER_STATS_CALCULATOR);
        step.setConfiguration(emptyStepConfig());

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(statsTablePrefix);
        step.setTargetTable(targetTable);

        return step;
    }

    private TransformationStepConfig filter(List<String> originalAttrs) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(BUCKET_STEP));
        step.setTransformer(TRANSFORMER_BUCKETED_FILTER);
        BucketedFilterConfig conf = new BucketedFilterConfig();
        conf.setOriginalAttrs(originalAttrs);
        String confStr = appendEngineConf(conf, baseEngineConfig());
        step.setConfiguration(confStr);
        return step;
    }

    private TransformationStepConfig sort(CustomerSpace customerSpace) {
        TransformationStepConfig step = new TransformationStepConfig();
        List<Integer> inputSteps = Collections.singletonList(FILTER_STEP);
        step.setInputSteps(inputSteps);
        step.setTransformer(TRANSFORMER_SORTER);

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(SORTED_TABLE_PREFIX);
        step.setTargetTable(targetTable);

        SorterConfig conf = new SorterConfig();
        conf.setPartitions(200);
        conf.setSplittingThreads(4);
        conf.setCompressResult(false);
        conf.setSortingField(DataCloudConstants.LATTICE_ACCOUNT_ID);
        String confStr = appendEngineConf(conf, baseEngineConfig());
        step.setConfiguration(confStr);
        return step;
    }

    private Map<MatchKey, List<String>> getKeyMap() {
        Map<MatchKey, List<String>> keyMap = new TreeMap<>();
        keyMap.put(MatchKey.LatticeAccountID, Collections.singletonList(DataCloudConstants.LATTICE_ACCOUNT_ID));
        return keyMap;
    }

    private void setProfileTable(String customerSpace, String profileTableName) {
        Table profileTable = metadataProxy.getTable(customerSpace, profileTableName);
        if (profileTable == null) {
            throw new RuntimeException("Failed to find profile table in customer " + customerSpace);
        }
        profileTable.setInterpretation(SchemaInterpretation.Profile.name());
        metadataProxy.updateTable(customerSpace, profileTableName, profileTable);

        DataCollectionType dataCollectionType = configuration.getDataCollectionType();
        DataCollection dataCollection = dataCollectionProxy.upsertTableToDataCollection(customerSpace,
                dataCollectionType, profileTableName, true);
        if (dataCollection == null) {
            throw new IllegalStateException("Failed to upsert profile table to data collection.");
        }

        profileTable = CDLWorkflowStepUtils.getProfileTable(dataCollection);
        if (profileTable == null) {
            throw new IllegalStateException("Cannot find the upsert profile table in data collection.");
        }
    }

}