package com.latticeengines.cdl.workflow.steps;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.CalculateStatsConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ProfileConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.SorterConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.SortContactStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;

@Component("sortContactStep")
public class SortContactStep extends ProfileStepBase<SortContactStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(SortContactStep.class);

    private static final String PROFILE_TABLE_PREFIX = "Profile";
    private static final String STATS_TABLE_PREFIX = "Stats";
    private static final String SORTED_TABLE_PREFIX = TableRoleInCollection.SortedContact.name();
    private static final List<String> masterTableSortKeys = TableRoleInCollection.SortedContact
            .getForeignKeysAsStringList();

    private static int profileStep;
    private static int bucketStep;

    private String[] dedupFields = { "AccountId" };

    @Override
    protected BusinessEntity getEntity() {
        return BusinessEntity.Contact;
    }

    @Override
    protected TransformationWorkflowConfiguration executePreTransformation() {
        String customerSpace = configuration.getCustomerSpace().toString();
        Table activeMasterTable = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.ConsolidatedContact);
        if (activeMasterTable == null) {
            throw new IllegalStateException("Cannot find the master table in default collection");
        }
        log.info(String.format("masterTableName for customer %s is %s", configuration.getCustomerSpace().toString(),
                activeMasterTable.getName()));
        PipelineTransformationRequest request = generateRequest(configuration.getCustomerSpace(), activeMasterTable);
        return transformationProxy.getWorkflowConf(request, configuration.getPodId());
    }

    @Override
    protected void onPostTransformationCompleted() {
        String profileTableName = TableUtils.getFullTableName(PROFILE_TABLE_PREFIX, pipelineVersion);
        String statsTableName = TableUtils.getFullTableName(STATS_TABLE_PREFIX, pipelineVersion);
        String sortedTableName = TableUtils.getFullTableName(SORTED_TABLE_PREFIX, pipelineVersion);

        upsertProfileTable(profileTableName, TableRoleInCollection.ContactProfile);

        Table sortedTable = metadataProxy.getTable(configuration.getCustomerSpace().toString(), sortedTableName);
        enrichTableSchema(sortedTable);
        metadataProxy.updateTable(configuration.getCustomerSpace().toString(), sortedTableName, sortedTable);

        updateEntityValueMapInContext(TABLE_GOING_TO_REDSHIFT, sortedTableName, String.class);
        updateEntityValueMapInContext(APPEND_TO_REDSHIFT_TABLE, false, Boolean.class);

        updateEntityValueMapInContext(SERVING_STORE_IN_STATS, sortedTableName, String.class);
        updateEntityValueMapInContext(STATS_TABLE_NAMES, statsTableName, String.class);
    }

    private PipelineTransformationRequest generateRequest(CustomerSpace customerSpace, Table masterTable) {
        String masterTableName = masterTable.getName();
        try {
            PipelineTransformationRequest request = new PipelineTransformationRequest();
            request.setName("ConsolidateContactStep");
            request.setSubmitter(customerSpace.getTenantId());
            request.setKeepTemp(false);
            request.setEnableSlack(false);

            profileStep = 0;
            bucketStep = 1;

            TransformationStepConfig profile = profile(customerSpace, masterTableName);
            TransformationStepConfig bucket = bucket(customerSpace, masterTableName);
            TransformationStepConfig calc = calcStats(customerSpace, STATS_TABLE_PREFIX);
            TransformationStepConfig sort = sort(customerSpace);
            TransformationStepConfig sortProfile = sortProfile(customerSpace, PROFILE_TABLE_PREFIX);
            // -----------
            List<TransformationStepConfig> steps = Arrays.asList( //
                    profile, //
                    bucket, //
                    calc, //
                    sort, //
                    sortProfile //
            );
            // -----------
            request.setSteps(steps);
            return request;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private TransformationStepConfig profile(CustomerSpace customerSpace, String sourceTableName) {
        TransformationStepConfig step = new TransformationStepConfig();
        String tableSourceName = "CustomerUniverse";
        SourceTable sourceTable = new SourceTable(sourceTableName, customerSpace);
        List<String> baseSources = Collections.singletonList(tableSourceName);
        step.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableSourceName, sourceTable);
        step.setBaseTables(baseTables);

        step.setTransformer(TRANSFORMER_PROFILER);
        ProfileConfig conf = new ProfileConfig();
        conf.setEncAttrPrefix(CEAttr);
        String confStr = appendEngineConf(conf, lightEngineConfig());
        step.setConfiguration(confStr);
        return step;
    }

    private TransformationStepConfig bucket(CustomerSpace customerSpace, String sourceTableName) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(profileStep));
        String tableSourceName = "CustomerUniverse";
        SourceTable sourceTable = new SourceTable(sourceTableName, customerSpace);
        List<String> baseSources = Collections.singletonList(tableSourceName);
        step.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableSourceName, sourceTable);
        step.setBaseTables(baseTables);

        step.setTransformer(TRANSFORMER_BUCKETER);
        step.setConfiguration(emptyStepConfig(lightEngineConfig()));
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

        CalculateStatsConfig conf = new CalculateStatsConfig();
        conf.setDedupFields(Arrays.asList(dedupFields));
        step.setConfiguration(appendEngineConf(conf, lightEngineConfig()));
        return step;
    }

    private TransformationStepConfig sort(CustomerSpace customerSpace) {
        TransformationStepConfig step = new TransformationStepConfig();
        List<Integer> inputSteps = Collections.singletonList(bucketStep);
        step.setInputSteps(inputSteps);

        step.setTransformer(TRANSFORMER_SORTER);

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(SORTED_TABLE_PREFIX);
        targetTable.setExpandBucketedAttrs(true);
        step.setTargetTable(targetTable);

        SorterConfig conf = new SorterConfig();
        conf.setPartitions(200);
        conf.setSplittingThreads(maxSplitThreads);
        conf.setSplittingChunkSize(10000L);
        conf.setCompressResult(true);
        conf.setSortingField(masterTableSortKeys.get(0)); // TODO: only support
                                                          // single sort key now
        String confStr = appendEngineConf(conf, heavyEngineConfig());
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

    private void enrichTableSchema(Table table) {
        List<Attribute> attrs = table.getAttributes();
        attrs.forEach(attr -> {
            attr.setCategory(Category.CONTACT_ATTRIBUTES);
            attr.removeAllowedDisplayNames();
        });
    }

}
